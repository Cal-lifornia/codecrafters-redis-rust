use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
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
        .write_all(&Resp::bulk_string_array(&[Bytes::from_static(b"PING")]).to_bytes())
        .await?;

    expect_simple_string(socket, "PONG").await?;

    socket
        .write_all(
            &Resp::bulk_string_array(&[
                Bytes::from_static(b"REPLCONF"),
                Bytes::from_static(b"listening-port"),
                Bytes::from(info.port),
            ])
            .to_bytes(),
        )
        .await?;

    expect_simple_string(socket, "OK").await?;
    socket
        .write_all(
            &Resp::bulk_string_array(&[
                Bytes::from_static(b"REPLCONF"),
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"psync2"),
            ])
            .to_bytes(),
        )
        .await?;

    expect_simple_string(socket, "OK").await?;

    socket
        .write_all(
            &Resp::bulk_string_array(&[
                Bytes::from_static(b"PSYNC"),
                Bytes::from(info.replication.replication_id),
                Bytes::from(info.replication.offset.to_string()),
            ])
            .to_bytes(),
        )
        .await?;
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

    if let Resp::SimpleString(_) = response {
        println!("got")
    } else {
        bail!("incorrect format")
    }

    Ok(())
}

pub async fn expect_simple_string(socket: &mut TcpStream, expecting: &str) -> Result<()> {
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
            bail!("expected {expecting}")
        } else {
            println!("got");
            Ok(())
        }
    } else {
        bail!("incorrect format")
    }
}
