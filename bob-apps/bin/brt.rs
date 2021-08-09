#[macro_use]
extern crate serde_derive;
use anyhow::{Context, Result as AnyResult};
use bincode::serialize_into;
use clap::{App, Arg, ArgMatches, SubCommand};
use crc::crc32::checksum_castagnoli as crc32;
use std::{
    error::Error,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, Instant},
};

const INPUT_OPT: &str = "input blob";
const OUTPUT_OPT: &str = "output blob";
const VALIDATE_EVERY_OPT: &str = "record cache";
const DISK_PATH_OPT: &str = "disk path";
const SUFFIX_OPT: &str = "suffix";
const BACKUP_SUFFIX_OPT: &str = "backup suffix";
const FIX_OPT: &str = "fix";
const NO_CONFIRM_OPT: &str = "no confirm";
const DELETE_OPT: &str = "index delete";
const VALIDATE_INDEX_COMMAND: &str = "validate-index";
const VALIDATE_BLOB_COMMAND: &str = "validate-blob";
const RECOVERY_COMMAND: &str = "recovery";
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

struct IndexReader {
    buf: Vec<u8>,
    position: u64,
}

impl IndexReader {
    fn from_path<P: AsRef<Path>>(path: P) -> AnyResult<Self> {
        let mut buf = vec![];
        OpenOptions::new()
            .read(true)
            .open(path)?
            .read_to_end(&mut buf)?;
        Ok(Self { buf, position: 0 })
    }

    fn is_eof(&self) -> bool {
        self.position >= self.buf.len() as u64
    }

    fn read_header(&mut self) -> AnyResult<IndexHeader> {
        let header: IndexHeader = bincode::deserialize(&self.buf[self.position as usize..])?;
        header.validate(&mut self.buf)?;
        self.position += bincode::serialized_size(&header)?;
        Ok(header)
    }

    fn read_filter(&mut self) -> AnyResult<BloomFilter> {
        let header: BloomFilter = bincode::deserialize(&self.buf[self.position as usize..])?;
        self.position += bincode::serialized_size(&header)?;
        Ok(header)
    }

    fn read_record_header(&mut self) -> AnyResult<Header> {
        let header: Header = bincode::deserialize(&self.buf[self.position as usize..])?;
        header.validate()?;
        self.position += bincode::serialized_size(&header)?;
        Ok(header)
    }
}

struct BlobReader {
    file: File,
    position: u64,
    len: u64,
}

impl BlobReader {
    fn from_path<P: AsRef<Path>>(path: P) -> AnyResult<Self> {
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
        let header: BlobHeader =
            bincode::deserialize_from(&mut self.file).with_context(|| "read blob header")?;
        header.validate().with_context(|| "validate blob header")?;
        self.position += bincode::serialized_size(&header)?;
        Ok(header)
    }

    fn read_record(&mut self) -> AnyResult<Record> {
        let header: Header =
            bincode::deserialize_from(&mut self.file).with_context(|| "read record header")?;
        header.validate()?;
        self.position += bincode::serialized_size(&header)?;

        let mut meta = vec![0; header.meta_size as usize];
        self.file
            .read_exact(&mut meta)
            .with_context(|| "read record meta")?;
        self.position += header.meta_size;

        let mut data = vec![0; header.data_size as usize];
        self.file
            .read_exact(&mut data)
            .with_context(|| "read record data")?;
        self.position += header.data_size;

        let record = Record { header, meta, data };
        record.validate().with_context(|| "validate record")?;
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
    fn from_path<P: AsRef<Path>>(path: P, should_cache_written: bool) -> AnyResult<Self> {
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
        self.written += bincode::serialized_size(&header).with_context(|| "write header")?;
        Ok(())
    }

    fn write_record(&mut self, record: Record) -> AnyResult<()> {
        bincode::serialize_into(&mut self.file, &record.header).with_context(|| "write header")?;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomConfig {
    pub elements: usize,
    pub hashers_count: usize,
    pub max_buf_bits_count: usize,
    pub buf_increase_step: usize,
    pub preferred_false_positive_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BloomFilter {
    config: BloomConfig,
    buf: Vec<usize>,
    bits_count: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct IndexHeader {
    records_count: usize,
    record_header_size: usize,
    filter_buf_size: usize,
    hash: Vec<u8>,
    version: u64,
    written: u8,
}

impl IndexHeader {
    fn validate(&self, buf: &mut Vec<u8>) -> AnyResult<()> {
        if self.written != 1 {
            return Err(ValidationError("Header is corrupt".to_string()).into());
        }
        if !self.hash_valid(buf)? {
            return Err(ValidationError("header hash mismatch".to_string()).into());
        }
        if self.version != 1 {
            return Err(ValidationError("header version mismatch".to_string()).into());
        }
        Ok(())
    }

    fn hash_valid(&self, buf: &mut Vec<u8>) -> AnyResult<bool> {
        let mut header = self.clone();
        let hash = header.hash.clone();
        header.hash = vec![0; ring::digest::SHA256.output_len];
        header.written = 0;
        serialize_into(buf.as_mut_slice(), &header)?;
        let new_hash = get_hash(&buf);
        Ok(hash == new_hash)
    }
}

fn get_hash(buf: &[u8]) -> Vec<u8> {
    use ring::digest::{Context, SHA256};
    let mut context = Context::new(&SHA256);
    context.update(buf);
    let digest = context.finish();
    digest.as_ref().to_vec()
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
        Settings::Validate(settings) => validate_blob_command(&settings),
        Settings::ValidateIndex(settings) => validate_index_command(&settings),
    }
}

fn validate_index(path: &Path) -> AnyResult<()> {
    let mut reader = IndexReader::from_path(path)?;
    let header = reader.read_header()?;
    reader.read_filter()?;
    for _ in 0..header.records_count {
        if reader.is_eof() {
            return Err(ValidationError("invalid records count, can't read more".into()).into());
        }
        reader.read_record_header()?;
    }
    Ok(())
}

fn validate_blob(path: &Path) -> AnyResult<()> {
    let mut reader = BlobReader::from_path(&path)?;
    reader.read_header()?;
    while !reader.is_eof() {
        reader.read_record()?;
    }
    Ok(())
}

fn validate_recursive<F>(path: &str, suffix: &str, mut validate: F) -> AnyResult<Vec<String>>
where
    F: FnMut(&Path) -> AnyResult<()> + Clone,
{
    log::info!("Start validation");
    let mut result = vec![];
    let mut count = 0;
    let mut last_log = Instant::now();
    for entry in std::fs::read_dir(path)?
        .into_iter()
        .filter_map(|res| match res {
            Ok(entry) => Some(entry),
            Err(err) => {
                log::error!("[{}] read dir error: {}", path, err);
                None
            }
        })
    {
        match entry.file_type() {
            Ok(file_type) if file_type.is_dir() => {
                result.extend(validate_recursive(path, suffix, validate.clone())?);
            }
            Ok(file_type) if file_type.is_file() => {
                let entry_path = entry.path().as_os_str().to_str().unwrap().to_string();
                if entry_path.ends_with(&suffix) {
                    match validate(entry_path.as_ref()) {
                        Ok(_) => {}
                        Err(err) => {
                            log::error!("[{}] validation error: {}", entry_path, err);
                            result.push(entry_path.to_string());
                        }
                    }
                    count += 1;
                    if last_log.elapsed() >= Duration::from_secs(1) {
                        log::info!("{} files validated", count);
                        last_log = Instant::now();
                    }
                }
            }
            Err(err) => {
                log::error!("[{}] file type error: {}", path, err);
            }
            _ => {}
        }
    }
    Ok(result)
}

fn ask_confirmation(prompt: &str) -> AnyResult<bool> {
    let mut stdout = std::io::stdout();
    let stdin = std::io::stdin();
    loop {
        let mut buf = String::new();
        write!(stdout, "{} (y/N): ", prompt)?;
        stdout.flush()?;
        stdin.read_line(&mut buf)?;
        let cmd = buf.trim();
        match cmd {
            "y" | "Y" => return Ok(true),
            "n" | "N" => return Ok(false),
            _ => writeln!(stdout, "Unknown command, try again")?,
        }
    }
}

fn move_and_recover_blob<P, Q>(
    invalid_path: P,
    backup_path: Q,
    validate_every: usize,
) -> AnyResult<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    std::fs::rename(&invalid_path, &backup_path).with_context(|| "move invalid blob")?;

    log::info!("backup saved to {:?}", backup_path.as_ref());
    recovery_blob(&backup_path, &invalid_path, validate_every)?;
    Ok(())
}

fn get_backup_path(path: &str, suffix: &str) -> AnyResult<PathBuf> {
    let mut i = 0;
    loop {
        let path = if i > 0 {
            PathBuf::from_str(&format!("{}{}.{}", path, suffix, i))
        } else {
            PathBuf::from_str(&path)
        }?;
        if !path.exists() {
            return Ok(path);
        }
        i += 1;
    }
}

fn validate_blob_command(settings: &ValidateBlobSettings) -> AnyResult<()> {
    let result = validate_recursive(&settings.path, &settings.blob_suffix, validate_blob)?;

    if !result.is_empty() {
        println!("Corrupted blobs:");
        for path in &result {
            println!("{}", path);
        }
    } else {
        println!("All blobs is valid");
    }

    if settings.fix && !result.is_empty() {
        if settings.confirm && !ask_confirmation("Fix invalid blob files?")? {
            return Ok(());
        }

        let mut fails = vec![];
        for path in &result {
            let backup_path = get_backup_path(path, &settings.backup_suffix)?;
            match move_and_recover_blob(&path, &backup_path, settings.validate_every) {
                Err(err) => {
                    log::error!("Error: {}", err);
                    fails.push(path);
                }
                _ => {
                    log::info!("[{}] recovered, backup saved to {:?}", path, backup_path);
                }
            }
        }
        if !fails.is_empty() {
            println!("Failed to recover these blobs:");
            for path in fails {
                println!("{}", path);
            }
        }
    }
    Ok(())
}

fn validate_index_command(settings: &ValidateIndexSettings) -> AnyResult<()> {
    let result = validate_recursive(&settings.path, &settings.index_suffix, validate_index)?;

    if !result.is_empty() {
        println!("Corrupted index files:");
        for path in &result {
            println!("{}", path);
        }
    } else {
        println!("All index files is valid");
    }

    if settings.delete && !result.is_empty() {
        if settings.confirm && !ask_confirmation("Delete invalid index files?")? {
            return Ok(());
        }

        let mut fails = vec![];
        for path in result {
            match std::fs::remove_file(&path) {
                Ok(_) => {
                    log::info!("[{}] index file removed", path);
                }
                Err(err) => {
                    log::info!("[{}] failed to remove index file: {}", path, err);
                    fails.push(path);
                }
            }
        }
        if !fails.is_empty() {
            println!("Failed to remove these files:");
            for path in fails {
                println!("{}", path);
            }
        }
    }
    Ok(())
}

fn recovery_blob<P, Q>(input: &P, output: &Q, validate_every: usize) -> AnyResult<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    if input.as_ref() == output.as_ref() {
        return Err(anyhow::anyhow!(
            "Recovering into same file is not supported"
        ));
    }
    let should_validate_written = validate_every != 0;
    let mut reader = BlobReader::from_path(&input)?;
    log::info!("Blob reader created");
    let header = reader.read_header()?;
    // Create writer after read blob header to prevent empty blob creation
    let mut writer = BlobWriter::from_path(&output, should_validate_written)?;
    log::info!("Blob writer created");
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
        if should_validate_written && count % validate_every == 0 {
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

fn recovery_command(settings: &RecoverySettings) -> AnyResult<()> {
    recovery_blob(&settings.input, &settings.output, settings.validate_every)
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
    Validate(ValidateBlobSettings),
    ValidateIndex(ValidateIndexSettings),
}

struct RecoverySettings {
    input: String,
    output: String,
    validate_every: usize,
}

struct ValidateBlobSettings {
    path: String,
    blob_suffix: String,
    backup_suffix: String,
    validate_every: usize,
    fix: bool,
    confirm: bool,
}

struct ValidateIndexSettings {
    path: String,
    index_suffix: String,
    delete: bool,
    confirm: bool,
}

impl Settings {
    fn from_matches() -> AnyResult<Settings> {
        let matches = get_matches();
        match matches.subcommand() {
            (RECOVERY_COMMAND, Some(matches)) => Ok(Settings::Recovery(RecoverySettings {
                input: matches.value_of(INPUT_OPT).expect("Required").to_string(),
                output: matches.value_of(OUTPUT_OPT).expect("Required").to_string(),
                validate_every: matches
                    .value_of(VALIDATE_EVERY_OPT)
                    .expect("Has default")
                    .parse()?,
            })),
            (VALIDATE_BLOB_COMMAND, Some(matches)) => {
                Ok(Settings::Validate(ValidateBlobSettings {
                    path: matches
                        .value_of(DISK_PATH_OPT)
                        .expect("Required")
                        .to_string(),
                    blob_suffix: matches.value_of(SUFFIX_OPT).expect("Required").to_string(),
                    backup_suffix: matches
                        .value_of(BACKUP_SUFFIX_OPT)
                        .expect("Required")
                        .to_string(),
                    fix: matches.is_present(FIX_OPT),
                    confirm: !matches.is_present(NO_CONFIRM_OPT),
                    validate_every: matches
                        .value_of(VALIDATE_EVERY_OPT)
                        .expect("Has default")
                        .parse()?,
                }))
            }
            (VALIDATE_INDEX_COMMAND, Some(matches)) => {
                Ok(Settings::ValidateIndex(ValidateIndexSettings {
                    path: matches
                        .value_of(DISK_PATH_OPT)
                        .expect("Required")
                        .to_string(),
                    index_suffix: matches.value_of(SUFFIX_OPT).expect("Required").to_string(),
                    delete: matches.is_present(DELETE_OPT),
                    confirm: !matches.is_present(NO_CONFIRM_OPT),
                }))
            }
            _ => Err(anyhow::anyhow!("Unknown command")),
        }
    }
}

fn get_matches<'a>() -> ArgMatches<'a> {
    let validate_command = SubCommand::with_name(VALIDATE_BLOB_COMMAND)
        .arg(
            Arg::with_name(DISK_PATH_OPT)
                .help("disk path")
                .short("p")
                .long("path")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name(SUFFIX_OPT)
                .help("blob suffix")
                .short("s")
                .long("suffix")
                .default_value("blob")
                .takes_value(true),
        )
        .arg(
            Arg::with_name(BACKUP_SUFFIX_OPT)
                .help("blob backup file suffix")
                .short("b")
                .long("backup-suffix")
                .default_value(".backup")
                .takes_value(true),
        )
        .arg(
            Arg::with_name(FIX_OPT)
                .takes_value(false)
                .help("fix invalid blob files")
                .short("f")
                .long("fix"),
        )
        .arg(
            Arg::with_name(NO_CONFIRM_OPT)
                .takes_value(false)
                .help("turn off fix confirmation")
                .long("no-confirm"),
        )
        .arg(
            Arg::with_name(VALIDATE_EVERY_OPT)
                .help("validate every N records")
                .takes_value(true)
                .default_value("100")
                .short("c")
                .value_name("N")
                .long("cache-size"),
        );
    let validate_index_command = SubCommand::with_name(VALIDATE_INDEX_COMMAND)
        .arg(
            Arg::with_name(DISK_PATH_OPT)
                .help("disk path")
                .short("p")
                .long("path")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name(SUFFIX_OPT)
                .help("index suffix")
                .short("s")
                .long("suffix")
                .default_value("index")
                .takes_value(true),
        )
        .arg(
            Arg::with_name(DELETE_OPT)
                .takes_value(false)
                .help("delete invalid index files")
                .short("d")
                .long("delete"),
        )
        .arg(
            Arg::with_name(NO_CONFIRM_OPT)
                .takes_value(false)
                .help("turn off fix confirmation")
                .long("no-confirm"),
        );
    let recovery_command = SubCommand::with_name(RECOVERY_COMMAND)
        .arg(
            Arg::with_name(INPUT_OPT)
                .help("input blob")
                .takes_value(true)
                .required(true)
                .short("i")
                .long("input"),
        )
        .arg(
            Arg::with_name(OUTPUT_OPT)
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
                .short("c")
                .value_name("N")
                .long("cache-size"),
        );

    App::new(format!("Blob recovery tool, {}", env!("CARGO_PKG_NAME")))
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand(recovery_command)
        .subcommand(validate_command)
        .subcommand(validate_index_command)
        .get_matches()
}

fn init_logger() -> AnyResult<()> {
    if std::env::var_os("RUST_LOG").is_none() {
        env_logger::builder()
            .filter(None, log::LevelFilter::Info)
            .try_init()?;
    } else {
        env_logger::try_init()?;
    }
    Ok(())
}
