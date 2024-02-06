use crate::hw_metrics_collector::{file_contents, CommandError};

const CPU_STAT_FILE: &str = "/proc/stat";

#[derive(Clone)]
pub(crate) struct CPUStatCollector {
    procfs_avl: bool,
}

impl CPUStatCollector {
    pub(crate) fn new() -> CPUStatCollector {
        CPUStatCollector {
            procfs_avl: true
        }
    }

    fn stat_cpu_line() -> Result<String, String> {
        let lines = file_contents(CPU_STAT_FILE)?;
        for stat_line in lines {
            if stat_line.starts_with("cpu") {
                return Ok(stat_line);
            }
        }
        Err(format!("Can't find cpu stat line in {}", CPU_STAT_FILE))
    }

    pub(crate) fn iowait(&mut self) -> Result<f64, CommandError> {
        if !self.procfs_avl {
            return Err(CommandError::Unavailable);
        }

        const CPU_IOWAIT_COLUMN: usize = 5;
        let mut err = None;
        match Self::stat_cpu_line() {
            Ok(line) => {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() > CPU_IOWAIT_COLUMN {
                    let mut sum = 0.;
                    let mut f_iowait = 0.;
                    for i in 1..parts.len() {
                        match parts[i].parse::<f64>() {
                            Ok(val) => {
                                sum += val;
                                if i == CPU_IOWAIT_COLUMN {
                                    f_iowait = val;
                                }
                            },
                            Err(_) => {
                                let msg = format!("Can't parse {}", CPU_STAT_FILE);
                                err = Some(msg);
                                break;
                            }
                        }
                    }
                    if err.is_none() {
                        return Ok(f_iowait * 100. / sum);
                    }
                } else {
                    let msg = format!("CPU stat format in {} changed", CPU_STAT_FILE);
                    err = Some(msg);
                }
            },
            Err(e) => {
                err = Some(e);
            }
        }
        self.procfs_avl = false;
        Err(CommandError::Primary(err.unwrap()))
    }
}

