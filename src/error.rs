use thiserror::Error as ErrorMacro;
use std::io;

#[derive(ErrorMacro, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    
    #[error("Data error: {0}")]
    DataError(String),
    
    #[error("Compression error: {0}")]
    CompressionError(String),
    
    #[error("Memory map error: {0}")]
    MemMapError(String),
    
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

