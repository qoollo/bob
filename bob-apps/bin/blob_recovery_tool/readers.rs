use super::prelude::*;

pub(crate) struct IndexReader {
    buf: Vec<u8>,
    position: u64,
}

impl IndexReader {
    pub(crate) fn from_path<P: AsRef<Path>>(path: P) -> AnyResult<Self> {
        let mut buf = vec![];
        OpenOptions::new()
            .read(true)
            .open(path)?
            .read_to_end(&mut buf)?;
        Ok(Self { buf, position: 0 })
    }

    pub(crate) fn is_eof(&self) -> bool {
        self.position >= self.buf.len() as u64
    }

    pub(crate) fn read_header(&mut self) -> AnyResult<IndexHeader> {
        let header: IndexHeader = bincode::deserialize(&self.buf[self.position as usize..])?;
        header.validate()?;
        header.hash_valid(&mut self.buf)?;
        self.position += bincode::serialized_size(&header)?;
        Ok(header)
    }

    pub(crate) fn read_filter(&mut self) -> AnyResult<BloomFilter> {
        let header: BloomFilter = bincode::deserialize(&self.buf[self.position as usize..])?;
        self.position += bincode::serialized_size(&header)?;
        Ok(header)
    }

    pub(crate) fn read_record_header(&mut self) -> AnyResult<Header> {
        let header: Header = bincode::deserialize(&self.buf[self.position as usize..])?;
        header.validate()?;
        self.position += bincode::serialized_size(&header)?;
        Ok(header)
    }
}

pub(crate) struct BlobReader {
    file: File,
    position: u64,
    len: u64,
    latest_wrong_header: Option<Header>,
}

impl BlobReader {
    pub(crate) fn from_path<P: AsRef<Path>>(path: P) -> AnyResult<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(BlobReader {
            len: file.metadata()?.len(),
            file,
            position: 0,
            latest_wrong_header: None,
        })
    }

    pub(crate) fn from_file(mut file: File) -> AnyResult<BlobReader> {
        let position = file.stream_position()?;
        let len = file.metadata()?.len();
        Ok(BlobReader {
            file,
            position,
            len,
            latest_wrong_header: None,
        })
    }

    pub(crate) fn is_eof(&self) -> bool {
        self.position >= self.len
    }

    fn read_data<T>(&mut self) -> AnyResult<T>
    where
        T: serde::de::DeserializeOwned + serde::Serialize,
    {
        let data = bincode::deserialize_from(&mut self.file)?;
        self.position += bincode::serialized_size(&data)?;
        Ok(data)
    }

    fn read_bytes(&mut self, count: usize) -> AnyResult<Vec<u8>> {
        let mut data = vec![0; count as usize];
        self.file
            .read_exact(&mut data)
            .with_context(|| "read record meta")?;
        self.position += count as u64;
        Ok(data)
    }

    pub(crate) fn read_header(&mut self) -> AnyResult<BlobHeader> {
        let header: BlobHeader = self.read_data().with_context(|| "read blob header")?;
        header.validate().with_context(|| "validate blob header")?;
        Ok(header)
    }

    pub(crate) fn read_record(&mut self) -> AnyResult<Record> {
        let header: Header =
            bincode::deserialize_from(&mut self.file).with_context(|| "read record header")?;
        self.position += bincode::serialized_size(&header)?;
        header.validate().map_err(|err| {
            self.latest_wrong_header = Some(header.clone());
            err
        })?;
        self.latest_wrong_header = None;

        let meta = self
            .read_bytes(header.meta_size as usize)
            .with_context(|| "read record meta")?;

        let data = self
            .read_bytes(header.data_size as usize)
            .with_context(|| "read record data")?;

        let record = Record { header, meta, data };
        record.validate().with_context(|| "validate record")?;
        Ok(record)
    }

    pub(crate) fn skip_wrong_record_data(&mut self) -> AnyResult<()> {
        debug!("Trying to skip wrong record data");
        let header = self
            .latest_wrong_header
            .as_ref()
            .ok_or_else(|| Error::skip_record_data_error("wrong header not found"))?;
        let position = self
            .position
            .checked_add(header.data_size)
            .and_then(|x| x.checked_add(header.meta_size))
            .ok_or_else(|| Error::skip_record_data_error("position overflow"))?;
        if position >= self.len {
            return Err(Error::skip_record_data_error("position is bigger than file size").into());
        }
        self.file.seek(SeekFrom::Start(position))?;
        debug!("Skipped {} bytes", position - self.position);
        self.position = position;
        Ok(())
    }

    pub(crate) fn read_record_with_skip_wrong(&mut self) -> AnyResult<Record> {
        match self.read_record() {
            Ok(record) => Ok(record),
            Err(error) => {
                match error.downcast_ref::<Error>() {
                    Some(Error::RecordValidation(_)) => {}
                    Some(Error::RecordHeaderValidation(_)) => self.skip_wrong_record_data()?,
                    _ => return Err(error),
                }
                warn!("Record read error, trying read next record: {}", error);
                self.read_record()
            }
        }
    }
}

pub(crate) struct BlobWriter {
    file: File,
    cache: Option<Vec<Record>>,
    written_cached: u64,
    written: u64,
}

impl BlobWriter {
    pub(crate) fn from_path<P: AsRef<Path>>(path: P, cache_written: bool) -> AnyResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let cache = if cache_written { Some(vec![]) } else { None };
        Ok(BlobWriter {
            file,
            written: 0,
            cache,
            written_cached: 0,
        })
    }

    pub(crate) fn written(&self) -> u64 {
        self.written
    }

    pub(crate) fn write_header(&mut self, header: &BlobHeader) -> AnyResult<()> {
        bincode::serialize_into(&mut self.file, header)?;
        self.written += bincode::serialized_size(&header).with_context(|| "write header")?;
        self.validate_written_header(header)?;
        Ok(())
    }

    pub(crate) fn write_record(&mut self, record: Record) -> AnyResult<()> {
        bincode::serialize_into(&mut self.file, &record.header).with_context(|| "write header")?;
        let mut written = 0;
        written += bincode::serialized_size(&record.header)?;
        self.file.write_all(&record.meta)?;
        written += record.meta.len() as u64;
        self.file.write_all(&record.data)?;
        written += record.data.len() as u64;
        debug!("Record written: {:?}", record);
        if let Some(cache) = &mut self.cache {
            cache.push(record);
            self.written_cached += written;
        }
        self.written += written;
        Ok(())
    }

    pub(crate) fn clear_cache(&mut self) {
        if let Some(cache) = &mut self.cache {
            cache.clear();
            self.written_cached = 0;
        }
    }

    pub(crate) fn validate_written_header(&mut self, written_header: &BlobHeader) -> AnyResult<()> {
        let current_position = self.written;
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        let mut reader = BlobReader::from_file(file)?;
        let header = reader.read_header()?;
        if header != *written_header {
            return Err(Error::blob_header_validation_error(
                "validation of written blob header failed",
            )
            .into());
        }
        self.file.seek(SeekFrom::Start(current_position))?;
        debug!("written header validated");
        Ok(())
    }

    pub(crate) fn validate_written_records(&mut self) -> AnyResult<()> {
        let cache = if let Some(cache) = &mut self.cache {
            cache
        } else {
            return Ok(());
        };
        if cache.is_empty() {
            return Ok(());
        }
        debug!("Start validation of written records");
        let current_position = self.written;
        let start_position = current_position
            .checked_sub(self.written_cached)
            .expect("Should be correct");
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(start_position))?;
        let mut reader = BlobReader::from_file(file)?;
        for record in cache.iter() {
            let written_record = reader.read_record()?;
            if record != &written_record {
                return Err(Error::record_validation_error(
                    "Written and cached records is not equal",
                )
                .into());
            }
        }
        self.file.seek(SeekFrom::Start(current_position))?;
        debug!("{} written records validated", cache.len());
        Ok(())
    }
}
