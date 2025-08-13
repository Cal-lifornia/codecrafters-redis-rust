use thiserror::Error;
use tokio::net::TcpStream;

use crate::mod_flat;

mod_flat!(handshake);

pub async fn connect_to_host(host_address: String) -> Result<(), Box<dyn std::error::Error>> {
    let mut connection = match TcpStream::connect(host_address).await {
        Ok(stream) => stream,
        Err(err) => return Err(Box::new(err)),
    };

    handshake(&mut connection).await?;

    Ok(())
}

#[derive(Debug, Error)]
pub enum ReplicaError {
    #[error("expecting {0}, got {1}")]
    UnexpectedResponse(String, String),
    #[error("error parsing input")]
    IncorrectFormat,
}
