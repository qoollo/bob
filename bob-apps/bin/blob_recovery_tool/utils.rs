use super::prelude::*;

pub(crate) fn init_logger() -> AnyResult<()> {
    if std::env::var_os("RUST_LOG").is_none() {
        env_logger::builder()
            .filter(None, log::LevelFilter::Info)
            .try_init()?;
    } else {
        env_logger::try_init()?;
    }
    Ok(())
}

pub(crate) fn get_hash(buf: &[u8]) -> Vec<u8> {
    use ring::digest::{Context, SHA256};
    let mut context = Context::new(&SHA256);
    context.update(buf);
    let digest = context.finish();
    digest.as_ref().to_vec()
}

pub(crate) fn validate_bytes(a: &[u8], checksum: u32) -> AnyResult<()> {
    let actual_checksum = calculate_checksum(a);
    if actual_checksum != checksum {
        return Err(Error::validation_error(format!(
            "wrong data checksum: '{}' != '{}'",
            actual_checksum, checksum
        ))
        .into());
    }
    Ok(())
}

pub(crate) fn calculate_checksum(a: &[u8]) -> u32 {
    crc32(a)
}

pub(crate) fn validate_bytes(a: &[u8], checksum: u32) -> bool {
    let actual_checksum = crc32(a);
    actual_checksum == checksum
}

pub(crate) fn get_backup_path(path: &str, suffix: &str) -> AnyResult<PathBuf> {
    let mut i = 0;
    loop {
        let path = if i > 0 {
            PathBuf::from_str(&format!("{}{}.{}", path, suffix, i))
        } else {
            PathBuf::from_str(path)
        }?;
        if !path.exists() {
            return Ok(path);
        }
        i += 1;
    }
}

pub(crate) fn ask_confirmation(prompt: &str) -> AnyResult<bool> {
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

pub(crate) fn validate_files_recursive<F>(
    path: &Path,
    suffix: &str,
    mut function: F,
) -> AnyResult<Vec<String>>
where
    F: FnMut(&Path) -> AnyResult<()> + Clone,
{
    info!("Start validation");
    let mut result = vec![];
    let mut count = 0;
    let mut last_log = Instant::now();
    for_each_file_recursive(path, |entry_path| {
        let entry_path = entry_path.as_os_str().to_str().unwrap().to_string();
        if entry_path.ends_with(&suffix) {
            if let Err(err) = function(entry_path.as_ref()) {
                error!("[{}] validation error: {}", entry_path, err);
                result.push(entry_path.to_string());
            }
            count += 1;
            if last_log.elapsed() >= Duration::from_secs(1) {
                info!("{} files validated", count);
                last_log = Instant::now();
            }
        }
    })?;
    info!("Validation completed! {} files validated", count);
    Ok(result)
}

pub(crate) fn for_each_file_recursive<F>(path: &Path, function: F) -> AnyResult<()>
where
    F: FnMut(&Path),
{
    let _ = for_each_file_recursive_inner(path, function)?;
    Ok(())
}

fn for_each_file_recursive_inner<F>(path: &Path, mut function: F) -> AnyResult<F>
where
    F: FnMut(&Path),
{
    for entry in std::fs::read_dir(path)?
        .into_iter()
        .filter_map(|res| match res {
            Ok(entry) => Some(entry),
            Err(err) => {
                error!("[{:?}] read dir error: {}", path, err);
                None
            }
        })
    {
        match entry.file_type() {
            Ok(file_type) if file_type.is_dir() => {
                function = for_each_file_recursive_inner(&entry.path(), function)?;
            }
            Ok(file_type) if file_type.is_file() => {
                function(&entry.path());
            }
            Err(err) => {
                error!("[{:?}] file type error: {}", path, err);
            }
            _ => {}
        }
    }
    Ok(function)
}

pub(crate) fn validate_index(path: &Path) -> AnyResult<()> {
    let mut reader = IndexReader::from_path(path)?;
    let header = reader.read_header()?;
    reader.read_filter()?;
    for _ in 0..header.records_count {
        if reader.is_eof() {
            return Err(
                Error::index_validation_error("invalid records count, can't read more").into(),
            );
        }
        reader.read_record_header()?;
    }
    Ok(())
}

pub(crate) fn validate_blob(path: &Path) -> AnyResult<()> {
    let mut reader = BlobReader::from_path(&path)?;
    reader.read_header()?;
    while !reader.is_eof() {
        reader.read_record()?;
    }
    Ok(())
}

pub(crate) fn move_and_recover_blob<P, Q>(
    invalid_path: P,
    backup_path: Q,
    validate_every: usize,
) -> AnyResult<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    std::fs::rename(&invalid_path, &backup_path).with_context(|| "move invalid blob")?;

    info!("backup saved to {:?}", backup_path.as_ref());
    recovery_blob(&backup_path, &invalid_path, validate_every)?;
    Ok(())
}

pub(crate) fn recovery_blob<P, Q>(input: &P, output: &Q, validate_every: usize) -> AnyResult<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    recovery_blob_with(input, output, validate_every, Ok)
}

pub(crate) fn recovery_blob_with<P, Q, F>(
    input: &P,
    output: &Q,
    validate_every: usize,
    preprocess_record: F,
) -> AnyResult<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
    F: Fn(Record) -> AnyResult<Record>,
{
    if input.as_ref() == output.as_ref() {
        return Err(anyhow::anyhow!(
            "Recovering into same file is not supported"
        ));
    }
    let validate_written_records = validate_every != 0;
    let mut reader = BlobReader::from_path(&input)?;
    info!("Blob reader created");
    let header = reader.read_header()?;
    // Create writer after read blob header to prevent empty blob creation
    let mut writer = BlobWriter::from_path(&output, validate_written_records)?;
    info!("Blob writer created");
    writer.write_header(&header)?;
    info!("Input blob header version: {}", header.version);
    let mut count = 0;
    while !reader.is_eof() {
        match reader
            .read_record_with_skip_wrong()
            .and_then(|record| preprocess_record(record))
        {
            Ok(record) => {
                writer.write_record(record)?;
                count += 1;
            }
            Err(error) => {
                warn!("Record read error: {}", error);
                break;
            }
        }
        if validate_written_records && count % validate_every == 0 {
            writer.validate_written_records()?;
            writer.clear_cache();
        }
    }
    if validate_written_records {
        writer.validate_written_records()?;
        writer.clear_cache();
    }
    info!(
        "Blob from '{:?}' recovered to '{:?}'",
        input.as_ref(),
        output.as_ref()
    );
    info!(
        "{} records written, totally {} bytes",
        count,
        writer.written()
    );
    Ok(())
}

pub(crate) fn print_result(result: &[String], file_type: &str) {
    if result.is_empty() {
        info!("All {} files is valid", file_type);
    } else {
        for value in result {
            info!("Print corrupted {} files", file_type);
            println!("{}", value);
        }
    }
}
