use std::{path::PathBuf, sync::Arc};

use either::Either;
use tokio::{net::TcpListener, sync::RwLock};

use crate::{
    account::{AccountDB, AccountError},
    command::CommandError,
    connection::Connection,
    context::{AppData, Config},
    database::{LocationError, RedisDatabase},
    rdb::RdbFile,
    redis_stream::StreamParseError,
    replica::{MainServer, Replica, ReplicaError, ReplicationInfo},
};

pub async fn run(
    port: Option<String>,
    replica: Option<String>,
    dir: Option<String>,
    db_file_name: Option<String>,
) -> anyhow::Result<()> {
    let port = port.unwrap_or("6379".into());
    let listener = TcpListener::bind(format! {"127.0.0.1:{}", port.clone()}).await?;
    let db = if let Some(file_name) = &db_file_name
        && let Some(dir) = &dir
    {
        let path: PathBuf = [dir, file_name].iter().collect();
        println!("PATH: {path:#?}");
        let rdb = RdbFile::read_file(path).await?;
        Arc::new(RedisDatabase::from_rdb(rdb.databases().first().cloned().unwrap_or(vec![])).await)
    } else {
        Arc::new(RedisDatabase::default())
    };
    let config = Arc::new(RwLock::new(Config::new(dir, db_file_name)));
    let mut info = ReplicationInfo::new(replica.is_none());
    let role = if let Some(main_address) = replica {
        Either::Right(Replica::connect(main_address, port).await?)
    } else {
        Either::Left(MainServer::default())
    };
    if let Either::Right(ref replica) = role {
        replica.handshake(&mut info).await?;
    }
    let replication = Arc::new(RwLock::new(info));
    let app_data = AppData {
        db,
        accounts: Arc::new(RwLock::new(AccountDB::default())),
        config,
        replication,
        role: role.clone(),
    };
    if let Either::Right(ref replica) = app_data.role {
        replica.conn.handle(true, app_data.clone()).await;
    }
    loop {
        let (socket, _) = listener.accept().await?;
        let connection = Connection::new(socket);
        connection.handle(false, app_data.clone()).await;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RedisError {
    #[error("{0}")]
    Account(#[from] AccountError),
    #[error("ERR {0}")]
    Command(#[from] CommandError),
    #[error("ERR {0}")]
    StreamParse(#[from] StreamParseError),
    #[error("ERR {0}")]
    Io(#[from] std::io::Error),
    #[error("ERR {0}")]
    Replica(#[from] ReplicaError),
    #[error("ERR {0}")]
    Location(#[from] LocationError),
    #[error("ERR {0}")]
    Other(String),
}

impl RedisError {
    pub fn other(value: impl std::fmt::Display) -> Self {
        Self::Other(value.to_string())
    }
}
