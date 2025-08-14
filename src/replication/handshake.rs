use anyhow::{anyhow, bail, Result};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    resp::{self, Resp},
    types::RedisInfo,
};

pub async fn handshake(socket: &mut TcpStream, info: RedisInfo) -> Result<()> {
    socket
        .write_all(&Resp::str_array(&["PING"]).to_bytes())
        .await?;

    expect_simple_string(socket, "PONG").await?;

    socket
        .write_all(&Resp::str_array(&["REPLCONF", "listening-port", &info.port]).to_bytes())
        .await?;

    expect_simple_string(socket, "OK").await?;
    socket
        .write_all(&Resp::str_array(&["REPLCONF", "capa", "psync2"]).to_bytes())
        .await?;

    expect_simple_string(socket, "OK").await?;

    socket
        .write_all(
            &Resp::str_array(&[
                "PSYNC",
                &info.replication.replication_id,
                &info.replication.offset.to_string(),
            ])
            .to_bytes(),
        )
        .await?;

    Ok(())
}

async fn expect_simple_string(socket: &mut TcpStream, expecting: &str) -> Result<()> {
    let mut buf = [0; 1024];
    let n = match socket.read(&mut buf).await {
        Ok(0) => {
            bail!("expected response")
        }
        Ok(n) => n,
        Err(err) => return Err(anyhow!(err)),
    };

    let response = match resp::parse(&buf[0..n]) {
        Ok((response, _)) => response,
        Err(err) => return Err(anyhow!(err)),
    };

    if let Resp::SimpleString(res) = response {
        if res != expecting {
            bail!("expected {res}")
        } else {
            Ok(())
        }
    } else {
        bail!("incorrect format")
    }
}
