use std::io;
use std::fmt;

#[derive(Debug)]
pub enum IndexError {
    /// I/O error
    Io(io::Error),
    /// Data is not in the expected indexed format
    InvalidFormat(&'static str),
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexError::Io(e) => write!(f, "I/O error: {}", e),
            IndexError::InvalidFormat(msg) => write!(f, "Invalid index format: {}", msg),
        }
    }
}

impl std::error::Error for IndexError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            IndexError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for IndexError {
    fn from(err: io::Error) -> IndexError {
        IndexError::Io(err)
    }
}

impl From<fst::Error> for IndexError {
    fn from(err: fst::Error) -> IndexError {
        IndexError::Io(io::Error::new(io::ErrorKind::Other, format!("fst error: {}", err)))
    }
}

pub type Result<T> = std::result::Result<T, IndexError>;
