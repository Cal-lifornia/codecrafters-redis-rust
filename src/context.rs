use std::sync::Arc;

use either::Either;
use tokio::net::tcp::OwnedWriteHalf;

use crate::{
    ArcLock,
    account::AccountDB,
    command::RedisCommand,
    database::RedisDatabase,
    replica::{MainServer, Replica, ReplicationInfo},
};

pub type ConnWriter = ArcLock<OwnedWriteHalf>;

#[derive(Clone)]
pub struct Context {
    pub writer: ArcLock<OwnedWriteHalf>,
    pub transactions: ArcLock<Option<Vec<RedisCommand>>>,
    pub signed_in: ArcLock<Option<usize>>,
    pub master_conn: bool,
    pub get_ack: ArcLock<bool>,
    pub app_data: AppData,
}

#[derive(Clone)]
pub struct AppData {
    pub db: Arc<RedisDatabase>,
    pub accounts: ArcLock<AccountDB>,
    pub config: ArcLock<Config>,
    pub replication: ArcLock<ReplicationInfo>,
    pub role: Either<MainServer, Replica>,
}

#[derive(Default, Clone)]
pub struct Config {
    pub dir: Option<String>,
    pub db_file_name: Option<String>,
}

impl Config {
    pub fn new(dir: Option<String>, db_file_name: Option<String>) -> Self {
        Self { dir, db_file_name }
    }
}
