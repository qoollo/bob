#[macro_use]
extern crate serde_derive;
use crc::crc32::checksum_castagnoli as crc32;
use std::{
    error::Error,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
};

use anyhow::Result as AnyResult;
use clap::{App, Arg, ArgMatches, SubCommand};

const INPUT_BLOB_OPT: &str = "input blob";
const OUTPUT_BLOB_OPT: &str = "output blob";
const VALIDATE_EVERY_OPT: &str = "record cache";
const DISK_PATH_OPT: &str = "disk path";
const BLOB_SUFFIX_OPT: &str = "blob suffix";
const RECORD_MAGIC_BYTE: u64 = 0xacdc_bcde;
const BLOB_MAGIC_BYTE: u64 = 0xdeaf_abcd;

#[derive(Debug)]
struct ValidationError(String);

impl Display for ValidationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "Validation error: {}", self.0)
    }
}

impl Error for ValidationError {}

struct BlobReader {
    file: File,
    position: u64,
    len: u64,
}

impl BlobReader {
    fn from_path(path: &str) -> AnyResult<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(BlobReader {
            len: file.metadata()?.len(),
            file,
            position: 0,
        })
    }

    fn from_file(mut file: File) -> AnyResult<BlobReader> {
        let position = file.seek(SeekFrom::Current(0))?;
        let len = file.metadata()?.len();
        Ok(BlobReader {
            file,
            position,
            len,
        })
    }

    fn is_eof(&self) -> bool {
        self.position >= self.len
    }

    fn read_header(&mut self) -> AnyResult<BlobHeader> {
        let header: BlobHeader = bincode::deserialize_from(&mut self.file)?;
        header.validate()?;
        self.position += bincode::serialized_size(&header)?;
        Ok(header)
    }

    fn read_record(&mut self) -> AnyResult<Record> {
        let header: Header = bincode::deserialize_from(&mut self.file)?;
        header.validate()?;
        self.position += bincode::serialized_size(&header)?;

        let mut meta = vec![0; header.meta_size as usize];
        self.file.read_exact(&mut meta)?;
        self.position += header.meta_size;

        let mut data = vec![0; header.data_size as usize];
        self.file.read_exact(&mut data)?;
        self.position += header.data_size;

        let record = Record { header, meta, data };
        record.validate()?;
        Ok(record)
    }
}

struct BlobWriter {
    file: File,
    cache: Option<Vec<Record>>,
    written_cached: u64,
    written: u64,
}

impl BlobWriter {
    fn from_path(path: &str, should_cache_written: bool) -> AnyResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let cache = if should_cache_written {
            Some(vec![])
        } else {
            None
        };
        Ok(BlobWriter {
            file,
            written: 0,
            cache,
            written_cached: 0,
        })
    }

    fn write_header(&mut self, header: &BlobHeader) -> AnyResult<()> {
        bincode::serialize_into(&mut self.file, header)?;
        self.written += bincode::serialized_size(&header)?;
        Ok(())
    }

    fn write_record(&mut self, record: Record) -> AnyResult<()> {
        bincode::serialize_into(&mut self.file, &record.header)?;
        let mut written = 0;
        written += bincode::serialized_size(&record.header)?;
        self.file.write_all(&record.meta)?;
        written += record.meta.len() as u64;
        self.file.write_all(&record.data)?;
        written += record.data.len() as u64;
        log::debug!("Record written: {:?}", record);
        if let Some(cache) = &mut self.cache {
            cache.push(record);
            self.written_cached += written;
        }
        self.written += written;
        Ok(())
    }

    fn clear_cache(&mut self) {
        if let Some(cache) = &mut self.cache {
            cache.clear();
            self.written_cached = 0;
        }
    }

    fn validate_written_records(&mut self) -> AnyResult<()> {
        let cache = if let Some(cache) = &mut self.cache {
            cache
        } else {
            return Ok(());
        };
        if cache.is_empty() {
            return Ok(());
        }
        log::debug!("Start validation of written records");
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
                return Err(
                    ValidationError("Written and cached records is not equal".to_string()).into(),
                );
            }
        }
        self.file.seek(SeekFrom::Start(current_position))?;
        log::debug!("{} written records validated", cache.len());
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct BlobHeader {
    magic_byte: u64,
    version: u32,
    flags: u64,
}

impl BlobHeader {
    fn validate(&self) -> AnyResult<()> {
        if self.magic_byte != BLOB_MAGIC_BYTE {
            return Err(ValidationError("blob header magic byte is invalid".to_string()).into());
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Header {
    magic_byte: u64,
    key: Vec<u8>,
    meta_size: u64,
    data_size: u64,
    flags: u8,
    blob_offset: u64,
    created: u64,
    data_checksum: u32,
    header_checksum: u32,
}

impl Header {
    fn validate(&self) -> AnyResult<()> {
        if self.magic_byte != RECORD_MAGIC_BYTE {
            return Err(ValidationError("record header magic byte is invalid".to_string()).into());
        }
        let mut header = self.clone();
        header.header_checksum = 0;
        let serialized = bincode::serialize(&header)?;
        validate_bytes(&serialized, self.header_checksum)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Default, Clone, PartialEq)]
pub struct Record {
    header: Header,
    meta: Vec<u8>,
    data: Vec<u8>,
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

impl Record {
    fn validate(&self) -> AnyResult<()> {
        self.header.validate()?;
        validate_bytes(&self.data, self.header.data_checksum)?;
        Ok(())
    }
}

fn main() {
    if let Err(err) = try_main() {
        log::error!("Program finished with error: {}", err);
    }
}

fn try_main() -> AnyResult<()> {
    init_logger()?;
    log::info!("Logger initialized");
    let settings = Settings::from_matches()?;
    match settings {
        Settings::Recovery(settings) => recovery_command(&settings),
        Settings::Validate(settings) => validate_command(&settings),
    }
}

fn validate_blob(path: &str) -> AnyResult<()> {
    let mut reader = BlobReader::from_path(path)?;
    reader.read_header()?;
    while !reader.is_eof() {
        reader.read_record()?;
    }
    Ok(())
}

fn validate_blobs_recursive(path: &str, settings: &ValidateSettings) -> AnyResult<Vec<String>> {
    let mut result = vec![];
    for entry in std::fs::read_dir(path)?
        .into_iter()
        .filter_map(|entry| match entry {
            Ok(entry) => Some(entry),
            Err(e) => {
                log::error!("[{}] read dir error: {}", path, e);
                None
            }
        })
    {
        let entry_path = entry.path().as_os_str().to_str().unwrap().to_string();
        match entry.file_type() {
            Ok(ftype) if ftype.is_dir() => {
                let invalid_blobs = match validate_blobs_recursive(&entry_path, settings) {
                    Ok(blobs) => blobs,
                    Err(err) => {
                        log::error!("[{}] validate blobs error: {}", entry_path, err);
                        vec![]
                    }
                };
                result.extend(invalid_blobs);
            }
            Ok(ftype) if ftype.is_file() && entry_path.ends_with(&settings.blob_suffix) => {
                match validate_blob(&entry_path) {
                    Ok(_) => {}
                    Err(err) => {
                        log::error!("[{}] blob validation error: {}", entry_path, err);
                        result.push(entry_path.to_string());
                    }
                }
            }
            Err(err) => {
                log::error!("[{}] filetype check error: {}", entry_path, err);
            }
            _ => {}
        }
    }
    Ok(result)
}

fn validate_command(settings: &ValidateSettings) -> AnyResult<()> {
    let result = validate_blobs_recursive(&settings.path, settings)?;
    eprintln!("Corrupted blobs:");
    for path in result {
        eprintln!("{}", path);
    }
    Ok(())
}

fn recovery_command(settings: &RecoverySettings) -> AnyResult<()> {
    let should_validate_written = settings.validate_every != 0;
    let mut reader = BlobReader::from_path(&settings.input)?;
    log::info!("Blob reader created");
    let mut writer = BlobWriter::from_path(&settings.output, should_validate_written)?;
    log::info!("Blob writer created");
    let header = reader.read_header()?;
    writer.write_header(&header)?;
    log::info!("Input blob header version: {}", header.version);
    let mut count = 0;
    while !reader.is_eof() {
        match reader.read_record() {
            Ok(record) => {
                writer.write_record(record)?;
                count += 1;
            }
            Err(error) => {
                log::info!("Record read error: {}", error);
                break;
            }
        }
        if should_validate_written && count % settings.validate_every == 0 {
            writer.validate_written_records()?;
            writer.clear_cache();
        }
    }
    if should_validate_written {
        writer.validate_written_records()?;
        writer.clear_cache();
    }
    log::info!(
        "{} records written, totally {} bytes",
        count,
        writer.written
    );
    Ok(())
}

fn validate_bytes(a: &[u8], checksum: u32) -> AnyResult<()> {
    let actual_checksum = crc32(&a);
    if actual_checksum != checksum {
        return Err(ValidationError(format!(
            "wrong data checksum: '{}' != '{}'",
            actual_checksum, checksum
        ))
        .into());
    }
    Ok(())
}

enum Settings {
    Recovery(RecoverySettings),
    Validate(ValidateSettings),
}

struct RecoverySettings {
    input: String,
    output: String,
    validate_every: usize,
}

struct ValidateSettings {
    path: String,
    blob_suffix: String,
}

impl Settings {
    fn from_matches() -> AnyResult<Settings> {
        let matches = get_matches();
        match matches.subcommand() {
            ("recovery", Some(matches)) => Ok(Settings::Recovery(RecoverySettings {
                input: matches
                    .value_of(INPUT_BLOB_OPT)
                    .expect("Required")
                    .to_string(),
                output: matches
                    .value_of(OUTPUT_BLOB_OPT)
                    .expect("Required")
                    .to_string(),
                validate_every: matches
                    .value_of(VALIDATE_EVERY_OPT)
                    .expect("Has default")
                    .parse()?,
            })),
            ("validate", Some(matches)) => Ok(Settings::Validate(ValidateSettings {
                path: matches
                    .value_of(DISK_PATH_OPT)
                    .expect("Required")
                    .to_string(),
                blob_suffix: matches
                    .value_of(BLOB_SUFFIX_OPT)
                    .expect("Required")
                    .to_string(),
            })),
            _ => Err(anyhow::anyhow!("Unknown command")),
        }
    }
}

fn get_matches<'a>() -> ArgMatches<'a> {
    let validate_command = SubCommand::with_name("validate")
        .arg(
            Arg::with_name(DISK_PATH_OPT)
                .help("disk path")
                .short("p")
                .long("path")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name(BLOB_SUFFIX_OPT)
                .help("blob suffix")
                .short("s")
                .long("suffix")
                .default_value("blob")
                .takes_value(true),
        );
    let recovery_command = SubCommand::with_name("recovery")
        .arg(
            Arg::with_name(INPUT_BLOB_OPT)
                .help("input blob")
                .takes_value(true)
                .required(true)
                .short("i")
                .long("input"),
        )
        .arg(
            Arg::with_name(OUTPUT_BLOB_OPT)
                .help("output blob")
                .takes_value(true)
                .required(true)
                .short("o")
                .long("output"),
        )
        .arg(
            Arg::with_name(VALIDATE_EVERY_OPT)
                .help("validate every N records")
                .takes_value(true)
                .default_value("100")
                .short("s")
                .value_name("N")
                .long("cache-size"),
        );

    App::new(format!("Blob recovery tool, {}", env!("CARGO_PKG_NAME")))
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand(recovery_command)
        .subcommand(validate_command)
        .get_matches()
}

fn init_logger() -> AnyResult<()> {
    env_logger::try_init()?;
    Ok(())
}
