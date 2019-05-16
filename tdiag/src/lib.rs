pub mod commands;

pub struct DiagError(pub String);

impl From<std::io::Error> for DiagError {
    fn from(error: std::io::Error) -> Self {
        DiagError(format!("io error: {}", error.to_string()))
    }
}

impl From<tdiag_connect::ConnectError> for DiagError {
    fn from(error: tdiag_connect::ConnectError) -> Self {
        match error {
            tdiag_connect::ConnectError::IoError(e) => DiagError(format!("io error: {}", e)),
            tdiag_connect::ConnectError::Other(e) => DiagError(e),
        }
    }
}
