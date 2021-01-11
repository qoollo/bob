//! A TCP Socket wrapper that reconnects automatically.
use crate::prelude::{IOError, IOErrorKind, IOResult};
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot::{channel, Receiver};
use tokio::time::interval;

const MIN_RECONNECT_DELAY_MS: u64 = 50;
const MAX_RECONNECT_DELAY_MS: u64 = 10_000;

#[derive(Debug)]
enum State {
    Socket(TcpStream),
    Task(Receiver<TcpStream>),
}

pub(super) struct RetrySocket {
    address: SocketAddr,
    socket: State,
}

impl fmt::Debug for RetrySocket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.socket.fmt(f)
    }
}

impl RetrySocket {
    pub(super) async fn new(address: SocketAddr) -> Self {
        let receiver = Self::spawn_task(address.clone());
        RetrySocket {
            address,
            socket: State::Task(receiver),
        }
    }

    fn spawn_task(address: SocketAddr) -> Receiver<TcpStream> {
        let (tx, rx) = channel();
        tokio::spawn(async move {
            let mut delay = MIN_RECONNECT_DELAY_MS;
            loop {
                if let Ok(stream) = TcpStream::connect(address).await {
                    if let Err(e) = tx.send(stream) {
                        warn!("Failed to send stream from task: {:?}", e);
                    }
                    return;
                }

                delay = MAX_RECONNECT_DELAY_MS.min(delay << 1);
                interval(Duration::from_millis(delay)).tick().await;
            }
        });
        rx
    }

    pub(super) fn check_connection(&mut self) -> Result<(), IOError> {
        match self.socket {
            State::Socket(_) => Ok(()),
            State::Task(ref mut rx) => {
                // TODO: process situation, when tx.send() fails in task (spawn new task)
                if let Ok(stream) = rx.try_recv() {
                    self.socket = State::Socket(stream);
                    warn!("Connected to {}!", self.address);
                    Ok(())
                } else {
                    Err(IOError::new(
                        IOErrorKind::NotConnected,
                        "Task is still pending",
                    ))
                }
            }
        }
    }

    pub(super) async fn write_all(&mut self, buf: &[u8]) -> IOResult<()> {
        match self.check_connection() {
            Err(e) => Err(e),
            Ok(_) => match self.socket {
                State::Socket(ref mut stream) => match stream.write_all(buf).await {
                    Ok(()) => Ok(()),
                    Err(e) if e.kind() == io::ErrorKind::BrokenPipe => {
                        warn!("Disconnected from {}", self.address);
                        self.socket = State::Task(Self::spawn_task(self.address));
                        Err(e)
                    }
                    Err(e) => Err(e),
                },
                State::Task(_) => Err(IOError::new(IOErrorKind::Other, "This code is unreachable")),
            },
        }
    }

    pub(super) async fn flush(&mut self) -> IOResult<()> {
        match self.check_connection() {
            Err(e) => Err(e),
            Ok(_) => match self.socket {
                State::Socket(ref mut stream) => match stream.flush().await {
                    Ok(()) => Ok(()),
                    Err(e) if e.kind() == io::ErrorKind::BrokenPipe => {
                        warn!("Disconnected from {}", self.address);
                        self.socket = State::Task(Self::spawn_task(self.address));
                        Err(e)
                    }
                    Err(e) => Err(e),
                },
                State::Task(_) => Err(IOError::new(IOErrorKind::Other, "This code is unreachable")),
            },
        }
    }
}
