use crate::ConnectError;

use timely::dataflow::operators::capture::EventReader;

use std::sync::{Arc, Mutex};
use std::net::{TcpStream, TcpListener, ToSocketAddrs, IpAddr};
use std::fs::File;
use std::path::PathBuf;

/// Listens on 127.0.0.1:8000 and opens `source_peers` sockets from the
/// computations we're examining (one socket for every worker on the
/// examined computation).
///
/// The sockets are wrapped in `Some(_)` because the result is commonly
/// used as a an argument to `make_readers` in this module.
///
/// The sockets are returned in nonblocking mode.
pub fn open_sockets(ip_addr: IpAddr, port: u16, source_peers: usize) -> Result<Vec<Option<TcpStream>>, ConnectError> {
    let listener = bind(ip_addr, port)?;
    await_sockets(listener, source_peers)
}

/// Wraps `TcpListener::bind` to return `ConnectError`s.
pub fn bind(ip_addr: IpAddr, port: u16) -> Result<TcpListener, ConnectError> {
    let socket_addr = (ip_addr, port).to_socket_addrs()?
        .next().ok_or(ConnectError::Other("Invalid listening address".to_string()))?;

    match TcpListener::bind(socket_addr) {
        Err(err) => Err(ConnectError::Other(err.to_string())),
        Ok(listener) => Ok(listener)
    }
}

/// Listens on the provided socket until `source_peers` connections
/// from the computations we're examining have been established (one
/// socket for every worker on the examined computation).
///
/// The sockets are wrapped in `Some(_)` because the result is
/// commonly used as a an argument to `make_readers` in this module.
///
/// The sockets are returned in nonblocking mode.
pub fn await_sockets(listener: TcpListener, source_peers: usize) -> Result<Vec<Option<TcpStream>>, ConnectError> {
    Ok((0..source_peers).map(|_| {
        let socket = listener.incoming().next().expect("Socket unexpectedly unavailable");
        if let Ok(ref s) = &socket {
            s.set_nonblocking(true)?;
        }
        socket.map(Some)
    }).collect::<Result<Vec<_>, _>>()?)
}

/// Types of Read created by `make_replayers`
pub enum TcpStreamOrFile {
    /// a TCP-backed online reader
    Tcp(TcpStream),
    /// a file-backed offline reader
    File(File),
}

impl std::io::Read for TcpStreamOrFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            TcpStreamOrFile::Tcp(x) => x.read(buf),
            TcpStreamOrFile::File(x) => x.read(buf),
        }
    }
}

/// Source of binary data for `make_replayers`.
#[derive(Clone)]
pub enum ReplaySource {
    Tcp(Arc<Mutex<Vec<Option<TcpStream>>>>),
    Files(Arc<Mutex<Vec<Option<PathBuf>>>>),
}

/// Construct EventReaders that read data from sockets or file
/// and can stream it into timely dataflow.
pub fn make_readers<T, E>(
    source: ReplaySource,
    worker_index: usize,
    worker_peers: usize,
    ) -> Result<Vec<EventReader<T, E, TcpStreamOrFile>>, ConnectError> {

    match source {
        ReplaySource::Tcp(sockets) => 
            Ok(sockets.lock().unwrap()
                .iter_mut().enumerate()
                .filter(|(i, _)| *i % worker_peers == worker_index)
                .map(|(_, s)| s.take().expect("socket missing, check the docs for make_replayers"))
                .map(|r| EventReader::<T, E, _>::new(TcpStreamOrFile::Tcp(r)))
                .collect::<Vec<_>>()),
        ReplaySource::Files(files) => {
            let open_files = files.lock().unwrap()
                .iter_mut().enumerate()
                .filter(|(i, _)| i % worker_peers == worker_index)
                .map(|(_, s)| s.take().expect("file name missing, check the docs for make_replayers"))
                .map(|p| File::open(&p))
                .collect::<Result<Vec<File>, std::io::Error>>()?;
            Ok(open_files.into_iter()
                .map(|f| EventReader::new(TcpStreamOrFile::File(f)))
                .collect::<Vec<_>>())
        }
    }
}
