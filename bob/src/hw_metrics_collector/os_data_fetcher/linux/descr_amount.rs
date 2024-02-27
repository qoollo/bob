use tokio::process::Command;

use super::parse_command_output;

const DESCRS_DIR: &str = "/proc/self/fd/";

// this constant means, that `descriptors amount` value will be recalculated only on every
// `CACHED_TIMES`-th `descr_amount` function call (on other hand last calculated (i.e. cached) value
// will be returned)
const CACHED_TIMES: usize = 10;

#[derive(Clone)]
pub(super) struct DescrCounter {
    value: u64,
    cached_times: usize,
    lsof_enabled: bool,
}

impl DescrCounter {
    pub(super) fn new() -> Self {
        DescrCounter {
            value: 0,
            cached_times: 0,
            lsof_enabled: true,
        }
    }

    pub(super) async fn descr_amount(&mut self) -> u64 {
        if self.cached_times == 0 {
            self.cached_times = CACHED_TIMES;
            self.value = self.count_descriptors().await;
        } else {
            self.cached_times -= 1;
        }
        self.value
    }

    async fn count_descriptors_by_lsof(&mut self) -> Option<u64> {
        if !self.lsof_enabled {
            return None;
        }

        let pid_arg = std::process::id().to_string();
        let cmd_lsof = Command::new("lsof")
            .args(["-a", "-p", &pid_arg, "-d", "^mem", "-d", "^cwd", "-d", "^rtd", "-d", "^txt", "-d", "^DEL"])
            .stdout(std::process::Stdio::piped())
            .spawn();
        match cmd_lsof {
            Ok(mut cmd_lsof) => {
                match cmd_lsof.stdout.take() {
                    Some(stdout) => {
                        match TryInto::<std::process::Stdio>::try_into(stdout) {
                            Ok(stdio) => {
                                match parse_command_output(Command::new("wc").arg("-l").stdin(stdio)).await {
                                    Ok(output) => {
                                        match output.trim().parse::<u64>() {
                                            Ok(count) => {
                                                if let Err(e) = cmd_lsof.wait().await {
                                                    debug!("lsof exited with error {}", e);
                                                }
                                                return Some(count - 5); // exclude stdin, stdout, stderr, lsof pipe and wc pipe
                                            }
                                            Err(e) => {
                                                debug!("failed to parse lsof result: {}", e);
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        debug!("can't use lsof, wc error (fs /proc will be used): {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                debug!("failed to parse stdout of lsof to stdio: {}", e);
                            }
                        }
                    },
                    None => {
                        debug!("lsof has no stdout (fs /proc will be used)");
                    }
                }
                if let Err(e) = cmd_lsof.wait().await {
                    debug!("lsof exited with error {}", e);
                }
            },
            Err(e) => {
                debug!("can't use lsof (fs /proc will be used): {}", e);
            }
        }
        self.lsof_enabled = false;
        None
    }

    async fn count_descriptors(&mut self) -> u64 {
        // FIXME: didn't find better way, but iterator's `count` method has O(n) complexity
        // isolated tests (notice that in this case directory may be cached, so it works more
        // quickly):
        // | fds amount | running (secs) |
        // | 1.000.000  |      0.6       |
        // |  500.000   |      0.29      |
        // |  250.000   |      0.15      |
        //
        //     for bob (tested on
        //                 Laptop: HP Pavilion Laptop 15-ck0xx,
        //                 OS: 5.12.16-1-MANJARO)
        //
        //  without payload:
        //  |  10.000   |      0.006     |
        //  with payload
        //  |  10.000   |      0.018     |
        if let Some(descr) = self.count_descriptors_by_lsof().await {
            return descr;
        }

        let d = std::fs::read_dir(DESCRS_DIR);
        match d {
            Ok(d) => {
                d.count() as u64 - 4 // exclude stdin, stdout, stderr and `read_dir` instance
            }
            Err(e) => {
                debug!("failed to count descriptors: {}", e);
                0 // proc is unsupported
            }
        }
    }
}
