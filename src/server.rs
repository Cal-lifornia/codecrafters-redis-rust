use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use either::Either;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::RwLock,
};

use crate::{
    command::{CommandError, get_command},
    context::Context,
    database::RedisDatabase,
    redis_stream::{RedisStream, StreamParseError},
    replica::{MainServer, Replica, ReplicaError, ReplicationInfo},
    resp::{RedisWrite, RespType},
};

pub async fn run(port: Option<String>, replica: Option<String>) -> Result<(), RedisError> {
    let port = port.unwrap_or("6379".into());
    let listener = TcpListener::bind(format! {"127.0.0.1:{}", port.clone()}).await?;
    let db = Arc::new(RedisDatabase::default());
    let mut info = ReplicationInfo::new(replica.is_none());
    let role = if let Some(main_address) = replica {
        Either::Right(Replica::new(main_address, port).await?)
    } else {
        Either::Left(MainServer::default())
    };
    if let Either::Right(ref replica) = role {
        replica.handshake(&mut info).await?;
    }
    let replication = Arc::new(RwLock::new(info));
    loop {
        let db_clone = db.clone();
        let (socket, _) = listener.accept().await?;
        let (mut read, write) = socket.into_split();
        let writer = Arc::new(RwLock::new(write));
        // let mut reader = BufReader::new(read);
        let ctx = Context {
            db: db_clone.clone(),
            writer,
            transactions: Arc::new(RwLock::new(None)),
            replication: replication.clone(),
            role: role.clone(),
        };
        tokio::spawn(async move {
            loop {
                let mut buf = [0u8; 1024];
                let n = match read.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(err) => {
                        eprintln!("failed to read from socket; err = {:?}", err);
                        return;
                    }
                };

                if let Err(err) = handle_stream(ctx.clone(), &buf[0..n]).await {
                    let mut results = BytesMut::new();
                    eprintln!("ERROR: {err}");
                    RespType::simple_error(err.to_string()).write_to_buf(&mut results);
                }
            }
        });
    }
}

async fn handle_stream(ctx: Context, stream: &[u8]) -> Result<(), RedisError> {
    let input = RespType::async_read(&mut BufReader::new(stream)).await?;
    let mut redis_stream = RedisStream::try_from(input)?;
    let mut buf = BytesMut::new();
    if let Some(next) = redis_stream.next() {
        let command = get_command(next, &mut redis_stream)?;
        let write = command.is_write_cmd();
        if let Either::Left(main) = &ctx.role
            && write
        {
            main.write_to_replicas(stream.to_vec()).await;
        }
        if (!matches!(
            command.name().to_lowercase().as_str(),
            "multi" | "exec" | "discard"
        )) && let Some(list) = ctx.transactions.write().await.as_mut()
        {
            RespType::simple_string("QUEUED").write_to_buf(&mut buf);
            list.push(command);
        } else {
            command.run_command(&ctx, &mut buf).await?
        }
        if !(ctx.role.is_right() && write) {
            let mut writer = ctx.writer.write().await;
            writer.write_all(&buf).await.expect("valid read");
        }
    } else {
        return Err(RedisError::Other("expected a value".into()));
    }

    Ok(())
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
    Replica(#[from] ReplicaError),
    #[error("{0}")]
    Other(String),
}
