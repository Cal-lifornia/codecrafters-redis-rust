use std::io::Error;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    replication::ReplicaError,
    resp::{self, Resp},
};

pub async fn handshake(socket: &mut TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    match socket
        .write_all(&Resp::StringArray(vec!["PING".to_string()]).to_bytes())
        .await
    {
        Ok(_) => Ok(()),
        Err(err) => Err(Box::new(err)),
    }

    // let mut buf = [0; 1024];
    // let n = match socket.read(&mut buf).await {
    //     Ok(0) => {
    //         return Err(Box::new(Error::new(
    //             std::io::ErrorKind::TimedOut,
    //             "no return after PING",
    //         )))
    //     }
    //     Ok(n) => n,
    //     Err(err) => return Err(Box::new(err)),
    // };

    // let response = match resp::parse(&buf[0..n]) {
    //     Ok((response, _)) => response,
    //     Err(err) => return Err(Box::new(err)),
    // };

    // if let Resp::SimpleString(res) = response {
    //     if res != "PONG" {
    //         Err(Box::new(ReplicaError::UnexpectedResponse(
    //             "PONG".to_string(),
    //             res,
    //         )))
    //     } else {
    //         Ok(())
    //     }
    // } else {
    //     Err(Box::new(ReplicaError::IncorrectFormat))
    // }
}
