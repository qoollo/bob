use super::prelude::*;

const RECORD_MAGIC_BYTE: u64 = 0xacdc_bcde;
const BLOB_MAGIC_BYTE: u64 = 0xdeaf_abcd;

pub(crate) trait Validatable {
    fn validate(&self) -> AnyResult<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BloomConfig {
    pub elements: usize,
    pub hashers_count: usize,
    pub max_buf_bits_count: usize,
    pub buf_increase_step: usize,
    pub preferred_false_positive_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BloomFilter {
    pub config: BloomConfig,
    pub buf: Vec<usize>,
    pub bits_count: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct IndexHeader {
    pub records_count: usize,
    pub record_header_size: usize,
    pub filter_buf_size: usize,
    pub hash: Vec<u8>,
    pub version: u64,
    pub written: u8,
}

impl Validatable for IndexHeader {
    fn validate(&self) -> AnyResult<()> {
        if self.written != 1 {
            return Err(
                Error::index_header_validation_error("Header is corrupt".to_string()).into(),
            );
        }
        if self.version != 1 {
            return Err(Error::index_header_validation_error(
                "header version mismatch".to_string(),
            )
            .into());
        }
        Ok(())
    }
}

impl IndexHeader {
    pub(crate) fn hash_valid(&self, buf: &mut Vec<u8>) -> AnyResult<bool> {
        let mut header = self.clone();
        let hash = header.hash.clone();
        header.hash = vec![0; ring::digest::SHA256.output_len];
        header.written = 0;
        serialize_into(buf.as_mut_slice(), &header)?;
        let new_hash = get_hash(buf);
        Ok(hash == new_hash)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub(crate) struct BlobHeader {
    pub magic_byte: u64,
    pub version: u32,
    pub flags: u64,
}

impl Validatable for BlobHeader {
    fn validate(&self) -> AnyResult<()> {
        if self.magic_byte != BLOB_MAGIC_BYTE {
            return Err(Error::blob_header_validation_error(
                "blob header magic byte is invalid".to_string(),
            )
            .into());
        }
        Ok(())
    }
}

impl BlobHeader {
    pub(crate) fn migrate(mut self, target: u32) -> AnyResult<Self> {
        self.version = target;
        Ok(self)
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub(crate) struct Header {
    pub magic_byte: u64,
    pub key: Vec<u8>,
    pub meta_size: u64,
    pub data_size: u64,
    pub flags: u8,
    pub blob_offset: u64,
    pub created: u64,
    pub data_checksum: u32,
    pub header_checksum: u32,
}

impl Header {
    pub(crate) fn with_reversed_key_bytes(mut self) -> AnyResult<Self> {
        self.key.reverse();
        self.update_checksum()?;
        Ok(self)
    }

    fn update_checksum(&mut self) -> AnyResult<()> {
        self.header_checksum = 0;
        let serialized = bincode::serialize(&self)?;
        self.header_checksum = calculate_checksum(&serialized);
        Ok(())
    }
}

impl Validatable for Header {
    fn validate(&self) -> AnyResult<()> {
        if self.magic_byte != RECORD_MAGIC_BYTE {
            return Err(Error::record_header_validation_error(
                "record header magic byte is invalid".to_string(),
            )
            .into());
        }
        let mut header = self.clone();
        header.header_checksum = 0;
        let serialized = bincode::serialize(&header)?;
        if !validate_bytes(&serialized, self.header_checksum) {
            return Err(Error::index_header_validation_error("invalid header checksum").into());
        };
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Default, Clone, PartialEq)]
pub(crate) struct Record {
    pub header: Header,
    pub meta: Vec<u8>,
    pub data: Vec<u8>,
}

impl Record {
    pub(crate) fn migrate(self, source: u32, target: u32) -> AnyResult<Self> {
        match (source, target) {
            (source, target) if source >= target => Ok(self),
            (0, 1) => self.mirgate_v0_to_v1(),
            (source, target) => Err(Error::unsupported_migration(source, target).into()),
        }
    }

    pub(crate) fn mirgate_v0_to_v1(mut self) -> AnyResult<Self> {
        self.header = self.header.with_reversed_key_bytes()?;
        Ok(self)
    }
}

impl Debug for Record {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "Record(meta_size={}, data_size={})",
            self.header.meta_size, self.header.data_size
        )
    }
}

impl Validatable for Record {
    fn validate(&self) -> AnyResult<()> {
        self.header.validate()?;
        if !validate_bytes(&self.data, self.header.data_checksum) {
            return Err(Error::record_validation_error("invalid data checksum").into());
        };
        Ok(())
    }
}
