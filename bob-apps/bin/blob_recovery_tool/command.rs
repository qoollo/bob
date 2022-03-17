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
const TARGET_VERSION_OPT: &str = "target version";
const SKIP_WRONG_OPT: &str = "skip wrong";

const VALIDATE_INDEX_COMMAND: &str = "validate-index";
const VALIDATE_BLOB_COMMAND: &str = "validate-blob";
const RECOVERY_COMMAND: &str = "recovery";
const MIGRATE_COMMAND: &str = "migrate";

pub enum MainCommand {
    Recovery(RecoveryBlobCommand),
    Validate(ValidateBlobCommand),
    ValidateIndex(ValidateIndexCommand),
    Migrate(MigrateCommand),
}

pub struct RecoveryBlobCommand {
    input: String,
    output: String,
    validate_every: usize,
    skip_wrong_record: bool,
}

impl RecoveryBlobCommand {
    fn run(&self) -> AnyResult<()> {
        recovery_blob(
            &self.input,
            &self.output,
            self.validate_every,
            self.skip_wrong_record,
        )
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
            .arg(
                Arg::with_name(SKIP_WRONG_OPT)
                    .takes_value(false)
                    .help("try to skip wrong records")
                    .long("skip-wrong-record"),
            )
    }

    fn from_matches(matches: &ArgMatches) -> AnyResult<RecoveryBlobCommand> {
        Ok(RecoveryBlobCommand {
            input: matches.value_of(INPUT_OPT).expect("Required").to_string(),
            output: matches.value_of(OUTPUT_OPT).expect("Required").to_string(),
            skip_wrong_record: matches.is_present(SKIP_WRONG_OPT),
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
    skip_confirmation: bool,
}

impl ValidateBlobCommand {
    fn run(&self) -> AnyResult<()> {
        if self.path.is_file() {
            validate_blob(&self.path)?;
            info!("Blob {:?} is valid", self.path);
            return Ok(());
        }
        let result = validate_files_recursive(&self.path, &self.blob_suffix, validate_blob)?;
        print_result(&result, "blob");

        if self.fix && !result.is_empty() {
            if !self.skip_confirmation && !ask_confirmation("Fix invalid blob files?")? {
                return Ok(());
            }

            for path in &result {
                let backup_path = get_backup_path(path, &self.backup_suffix)?;
                match move_and_recover_blob(&path, &backup_path, self.validate_every) {
                    Err(err) => {
                        error!("Error: {}", err);
                    }
                    _ => {
                        info!("[{}] recovered, backup saved to {:?}", path, backup_path);
                    }
                }
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
            skip_confirmation: matches.is_present(NO_CONFIRM_OPT),
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
    skip_confirmation: bool,
}

impl ValidateIndexCommand {
    fn run(&self) -> AnyResult<()> {
        if self.path.is_file() {
            validate_index(&self.path)?;
            info!("Index {:?} is valid", self.path);
            return Ok(());
        }
        let result = validate_files_recursive(&self.path, &self.index_suffix, validate_index)?;
        print_result(&result, "index");

        if self.delete && !result.is_empty() {
            if !self.skip_confirmation && !ask_confirmation("Delete invalid index files?")? {
                return Ok(());
            }

            for path in result {
                match std::fs::remove_file(&path) {
                    Ok(_) => {
                        info!("[{}] index file removed", path);
                    }
                    Err(err) => {
                        info!("[{}] failed to remove index file: {}", path, err);
                    }
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
            skip_confirmation: matches.is_present(NO_CONFIRM_OPT),
        })
    }
}

pub struct MigrateCommand {
    input_path: PathBuf,
    output_path: PathBuf,
    blob_suffix: String,
    validate_every: usize,
    target_version: u32,
}

impl MigrateCommand {
    fn run(&self) -> AnyResult<()> {
        if self.input_path.is_file() {
            self.migrate_file(&self.input_path, &self.output_path)?;
        } else {
            process_files_recursive(
                &self.input_path,
                &self.blob_suffix,
                |path, relative_path| {
                    let output_dir = self.output_path.join(relative_path);
                    std::fs::create_dir_all(&output_dir)?;
                    let output = output_dir.join(path.file_name().expect("Must be filename"));
                    self.migrate_file(path, &output)
                },
                "Migration",
            )?;
        }
        Ok(())
    }

    fn migrate_file(&self, input: &Path, output: &Path) -> AnyResult<()> {
        recovery_blob_with(
            &input,
            &output,
            self.validate_every,
            |header| header.migrate(self.target_version), 
            |record, version| record.migrate(version, self.target_version),
            false,
        )?;
        Ok(())
    }

    fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name(MIGRATE_COMMAND)
            .arg(
                Arg::with_name(INPUT_OPT)
                    .help("input disk path")
                    .value_name("path")
                    .short("i")
                    .long("input")
                    .takes_value(true)
                    .required(true),
            )
            .arg(
                Arg::with_name(OUTPUT_OPT)
                    .help("output disk path")
                    .value_name("path")
                    .short("o")
                    .long("output")
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
                Arg::with_name(VALIDATE_EVERY_OPT)
                    .help("validate every N records")
                    .takes_value(true)
                    .default_value("100")
                    .short("c")
                    .value_name("N")
                    .long("cache-size"),
            )
            .arg(
                Arg::with_name(TARGET_VERSION_OPT)
                    .help("target blob version for migration")
                    .takes_value(true)
                    .default_value("1")
                    .short("t")
                    .value_name("version")
                    .long("target-version"),
            )
    }

    fn from_matches(matches: &ArgMatches) -> AnyResult<MigrateCommand> {
        Ok(MigrateCommand {
            input_path: matches.value_of(INPUT_OPT).expect("Required").into(),
            output_path: matches.value_of(OUTPUT_OPT).expect("Required").into(),
            blob_suffix: matches.value_of(SUFFIX_OPT).expect("Required").to_string(),
            validate_every: matches
                .value_of(VALIDATE_EVERY_OPT)
                .expect("Has default")
                .parse()?,
            target_version: matches
                .value_of(TARGET_VERSION_OPT)
                .expect("Has default")
                .parse()?,
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
            MainCommand::Migrate(settings) => settings.run(),
        }
    }

    fn get_matches<'a>() -> ArgMatches<'a> {
        App::new(format!("Blob recovery tool, {}", env!("CARGO_PKG_NAME")))
            .version(env!("CARGO_PKG_VERSION"))
            .subcommand(RecoveryBlobCommand::subcommand())
            .subcommand(ValidateBlobCommand::subcommand())
            .subcommand(ValidateIndexCommand::subcommand())
            .subcommand(MigrateCommand::subcommand())
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
            (MIGRATE_COMMAND, Some(matches)) => {
                Ok(MainCommand::Migrate(MigrateCommand::from_matches(matches)?))
            }
            _ => Err(anyhow::anyhow!("Unknown command")),
        }
    }
}
