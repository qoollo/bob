#[macro_use]
extern crate serde_derive;
use crc::crc32::checksum_castagnoli as crc32;
use std::{
    error::Error,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    fs::{File, OpenOptions},
    io::{Read, Write},
};

use anyhow::Result as AnyResult;
use clap::{App, Arg, ArgMatches};

const INPUT_BLOB_OPT: &str = "input blob";
const OUTPUT_BLOB_OPT: &str = "output blob";
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

struct Blob {
    file: File,
    position: u64,
    len: u64,
}

impl Blob {
    fn reader(path: &str) -> AnyResult<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(Blob {
            len: file.metadata()?.len(),
            file,
            position: 0,
        })
    }

    fn writer(path: &str) -> AnyResult<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Blob {
            file,
            position: 0,
            len: 0,
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

    fn write_header(&mut self, header: &BlobHeader) -> AnyResult<()> {
        bincode::serialize_into(&mut self.file, header)?;
        self.position += bincode::serialized_size(&header)?;
        Ok(())
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

    fn write_record(&mut self, record: &Record) -> AnyResult<()> {
        bincode::serialize_into(&mut self.file, &record.header)?;
        self.position += bincode::serialized_size(&record.header)?;
        self.file.write_all(&record.meta)?;
        self.position += record.meta.len() as u64;
        self.file.write_all(&record.data)?;
        self.position += record.data.len() as u64;
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
        let checksum = crc32(&serialized);
        if self.header_checksum != checksum {
            return Err(ValidationError(format!(
                "wrong header checksum: '{}' != '{}'",
                self.header_checksum, checksum
            ))
            .into());
        }
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
        let checksum = crc32(&self.data);
        if self.header.data_checksum != checksum {
            return Err(ValidationError(format!(
                "wrong data checksum: '{}' != '{}'",
                self.header.data_checksum, checksum
            ))
            .into());
        }
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
    let mut input = Blob::reader(&settings.input)?;
    log::info!("Blob reader created");
    let mut output = Blob::writer(&settings.output)?;
    log::info!("Blob writer created");
    let header = input.read_header()?;
    output.write_header(&header)?;
    log::info!("Input blob header version: {}", header.version);
    let mut count = 0;
    while !input.is_eof() {
        match input.read_record() {
            Ok(record) => {
                output.write_record(&record)?;
                log::debug!("Record written: {:?}", record);
                count += 1;
            }
            Err(error) => {
                log::info!("Record read error: {}", error);
                break;
            }
        }
    }
    log::info!(
        "{} records written, totally {} bytes",
        count,
        output.position
    );
    Ok(())
}

struct Settings {
    input: String,
    output: String,
}

impl Settings {
    fn from_matches() -> AnyResult<Settings> {
        let matches = get_matches();
        Ok(Settings {
            input: matches
                .value_of(INPUT_BLOB_OPT)
                .expect("Required")
                .to_string(),
            output: matches
                .value_of(OUTPUT_BLOB_OPT)
                .expect("Required")
                .to_string(),
        })
    }
}

fn get_matches<'a>() -> ArgMatches<'a> {
    App::new(format!("Blob recovery tool, {}", env!("CARGO_PKG_NAME")))
        .version(env!("CARGO_PKG_VERSION"))
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
        .get_matches()
}

fn init_logger() -> AnyResult<()> {
    env_logger::try_init()?;
    Ok(())
}
