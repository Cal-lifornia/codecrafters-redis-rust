use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::{
    command::{
        AsyncCommand, Blpop, Command, CommandError, Echo, Get, LLen, Lpop, Lpush, Lrange, Rpush,
        Set, TypeCmd, Xadd, Xrange,
    },
    context::Context,
    database::RedisDatabase,
    redis_stream::{RedisStream, StreamParseError},
    resp::{RedisWrite, RespType},
};

pub async fn run() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let db = Arc::new(RedisDatabase::default());

    loop {
        let db_clone = db.clone();
        let (mut socket, _) = listener.accept().await?;
        let ctx = Context {
            db: db_clone.clone(),
        };
        tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                let n = match socket.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(err) => {
                        eprintln!("failed to read from socket; err = {:?}", err);
                        return;
                    }
                };
                let results = match handle_stream(ctx.clone(), &buf[0..n]).await {
                    Ok(results) => results,
                    Err(err) => {
                        let mut results = BytesMut::new();
                        eprintln!("ERROR: {err}");
                        RespType::simple_error(err.to_string()).write_to_buf(&mut results);
                        results.into()
                    }
                };
                socket.write_all(&results).await.expect("valid read");
            }
        });
    }
}

async fn handle_stream(ctx: Context, stream: &[u8]) -> Result<Bytes, RedisError> {
    let mut buf = BytesMut::new();
    let (result, _) = RespType::from_utf8(stream)?;
    let mut redis_stream = RedisStream::try_from(result)?;
    if let Some(next) = redis_stream.next() {
        match next.to_ascii_lowercase().as_slice() {
            b"ping" => RespType::simple_string("PONG".into()).write_to_buf(&mut buf),
            b"echo" => {
                Echo::parse(&mut redis_stream)?
                    .run_command(&ctx, &mut buf)
                    .await?
            }
            b"type" => {
                TypeCmd::parse(&mut redis_stream)?
                    .run_command(&ctx, &mut buf)
                    .await?
            }
            b"set" => {
                Set::parse(&mut redis_stream)?
                    .run_command(&ctx, &mut buf)
                    .await?
            }
            b"get" => {
                Get::parse(&mut redis_stream)?
                    .run_command(&ctx, &mut buf)
                    .await?
            }
            b"rpush" => {
                Rpush::parse(&mut redis_stream)?
                    .run_command(&ctx, &mut buf)
                    .await?
            }
            b"lrange" => {
                Lrange::parse(&mut redis_stream)?
                    .run_command(&ctx, &mut buf)
                    .await?
            }
            b"lpush" => {
                Lpush::parse(&mut redis_stream)?
                    .run_command(&ctx, &mut buf)
                    .await?
            }
            b"llen" => {
                LLen::parse(&mut redis_stream)?
                    .run_command(&ctx, &mut buf)
                    .await?
            }
            b"lpop" => {
                Lpop::parse(&mut redis_stream)?
                    .run_command(&ctx, &mut buf)
                    .await?
            }
            b"blpop" => {
                Blpop::parse(&mut redis_stream)?
                    .run_command(&ctx, &mut buf)
                    .await?
            }
            b"xadd" => {
                Xadd::parse(&mut redis_stream)?
                    .run_command(&ctx, &mut buf)
                    .await?
            }
            b"xrange" => {
                Xrange::parse(&mut redis_stream)?
                    .run_command(&ctx, &mut buf)
                    .await?
            }
            _ => todo!(),
        }
    } else {
        return Err(RedisError::Other("expected a value".into()));
    }

    Ok(buf.into())
}

#[derive(Debug, thiserror::Error)]
pub enum RedisError {
    #[error("{0}")]
    Command(#[from] CommandError),
    #[error("{0}")]
    StreamParse(#[from] StreamParseError),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}
