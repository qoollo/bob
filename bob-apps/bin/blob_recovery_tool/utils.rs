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

pub(crate) fn process_files_recursive<F>(
    path: &Path,
    extension: &str,
    mut function: F,
    prefix: &str,
) -> AnyResult<Vec<String>>
where
    F: FnMut(&Path, &Path) -> AnyResult<()>,
{
    warn!("{}: start", prefix);
    let mut result = vec![];
    let mut count = 0;
    let mut last_log = Instant::now();
    for_each_file_recursive(path, |entry_path, relative_path| {
        let path_string = entry_path.as_os_str().to_str().unwrap().to_string();
        if path_string.ends_with(&extension) {
            match function(entry_path, relative_path) {
                Ok(_) => {}
                Err(err) => {
                    error!("{}: [{}] error: {}", prefix, path_string, err);
                    result.push(path_string);
                }
            }
            count += 1;
            if last_log.elapsed() >= Duration::from_secs(1) {
                warn!("{}: {} files processed", prefix, count);
                last_log = Instant::now();
            }
        }
    })?;
    warn!("{} completed! {} files processed", prefix, count);
    Ok(result)
}

pub(crate) fn for_each_file_recursive<F>(path: &Path, function: F) -> AnyResult<()>
where
    F: FnMut(&Path, &Path),
{
    let mut relative_path = PathBuf::new();
    let _ = for_each_file_recursive_inner(path, function, &mut relative_path)?;
    Ok(())
}

fn for_each_file_recursive_inner<F>(
    path: &Path,
    mut function: F,
    relative_path: &mut PathBuf,
) -> AnyResult<F>
where
    F: FnMut(&Path, &Path),
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
                relative_path.push(entry.file_name());
                function = for_each_file_recursive_inner(&entry.path(), function, relative_path)?;
                relative_path.pop();
            }
            Ok(file_type) if file_type.is_file() => {
                function(&entry.path(), relative_path);
            }
            Err(err) => {
                error!("[{:?}] file type error: {}", path, err);
            }
            _ => {}
        }
    }
    Ok(function)
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
    for_each_file_recursive(path, |entry_path, _relative_path| {
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
