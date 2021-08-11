use super::prelude::*;

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

pub enum MainCommand {
    Recovery(RecoveryBlobCommand),
    Validate(ValidateBlobCommand),
    ValidateIndex(ValidateIndexCommand),
}

pub struct RecoveryBlobCommand {
    input: String,
    output: String,
    validate_every: usize,
}

impl RecoveryBlobCommand {
    fn run(&self) -> AnyResult<()> {
        recovery_blob(&self.input, &self.output, self.validate_every)
    }

    fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name(RECOVERY_COMMAND)
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
            )
    }

    fn from_matches(matches: &ArgMatches) -> AnyResult<RecoveryBlobCommand> {
        Ok(RecoveryBlobCommand {
            input: matches.value_of(INPUT_OPT).expect("Required").to_string(),
            output: matches.value_of(OUTPUT_OPT).expect("Required").to_string(),
            validate_every: matches
                .value_of(VALIDATE_EVERY_OPT)
                .expect("Has default")
                .parse()?,
        })
    }
}

pub struct ValidateBlobCommand {
    path: PathBuf,
    blob_suffix: String,
    backup_suffix: String,
    validate_every: usize,
    fix: bool,
    confirm: bool,
}

impl ValidateBlobCommand {
    fn run(&self) -> AnyResult<()> {
        let result = validate_files_recursive(&self.path, &self.blob_suffix, validate_blob)?;

        if !result.is_empty() {
            if !self.fix {
                log::info!("Print corrupted blobs");
                for path in &result {
                    println!("{}", path);
                }
            }
        } else {
            log::info!("All blobs is valid");
        }

        if self.fix && !result.is_empty() {
            if self.confirm && !ask_confirmation("Fix invalid blob files?")? {
                return Ok(());
            }

            let mut fails = vec![];
            for path in &result {
                let backup_path = get_backup_path(path, &self.backup_suffix)?;
                match move_and_recover_blob(&path, &backup_path, self.validate_every) {
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
                log::info!("Print not recovered blobs");
                for path in fails {
                    println!("{}", path);
                }
            } else {
                log::info!("All blobs recovered");
            }
        }
        Ok(())
    }

    fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name(VALIDATE_BLOB_COMMAND)
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
            )
    }

    fn from_matches(matches: &ArgMatches) -> AnyResult<ValidateBlobCommand> {
        Ok(ValidateBlobCommand {
            path: matches.value_of(DISK_PATH_OPT).expect("Required").into(),
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
        })
    }
}

pub struct ValidateIndexCommand {
    path: PathBuf,
    index_suffix: String,
    delete: bool,
    confirm: bool,
}

impl ValidateIndexCommand {
    fn run(&self) -> AnyResult<()> {
        let result = validate_files_recursive(&self.path, &self.index_suffix, validate_index)?;

        if !result.is_empty() {
            println!("Corrupted index files:");
            for path in &result {
                println!("{}", path);
            }
        } else {
            println!("All index files is valid");
        }

        if self.delete && !result.is_empty() {
            if self.confirm && !ask_confirmation("Delete invalid index files?")? {
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

    fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name(VALIDATE_INDEX_COMMAND)
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
            )
    }

    fn from_matches(matches: &ArgMatches) -> AnyResult<Self> {
        Ok(ValidateIndexCommand {
            path: matches.value_of(DISK_PATH_OPT).expect("Required").into(),
            index_suffix: matches.value_of(SUFFIX_OPT).expect("Required").to_string(),
            delete: matches.is_present(DELETE_OPT),
            confirm: !matches.is_present(NO_CONFIRM_OPT),
        })
    }
}

impl MainCommand {
    pub fn run() -> AnyResult<()> {
        let settings = MainCommand::from_matches()?;
        match settings {
            MainCommand::Recovery(settings) => settings.run(),
            MainCommand::Validate(settings) => settings.run(),
            MainCommand::ValidateIndex(settings) => settings.run(),
        }
    }

    fn get_matches<'a>() -> ArgMatches<'a> {
        App::new(format!("Blob recovery tool, {}", env!("CARGO_PKG_NAME")))
            .version(env!("CARGO_PKG_VERSION"))
            .subcommand(RecoveryBlobCommand::subcommand())
            .subcommand(ValidateBlobCommand::subcommand())
            .subcommand(ValidateIndexCommand::subcommand())
            .get_matches()
    }

    fn from_matches() -> AnyResult<MainCommand> {
        let matches = Self::get_matches();
        match matches.subcommand() {
            (RECOVERY_COMMAND, Some(matches)) => Ok(MainCommand::Recovery(
                RecoveryBlobCommand::from_matches(matches)?,
            )),
            (VALIDATE_BLOB_COMMAND, Some(matches)) => Ok(MainCommand::Validate(
                ValidateBlobCommand::from_matches(matches)?,
            )),
            (VALIDATE_INDEX_COMMAND, Some(matches)) => Ok(MainCommand::ValidateIndex(
                ValidateIndexCommand::from_matches(matches)?,
            )),
            _ => Err(anyhow::anyhow!("Unknown command")),
        }
    }
}
