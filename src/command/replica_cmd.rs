use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    command::{AsyncCommand, Command, CommandError},
    rdb::RdbFile,
    redis_stream::StreamParseError,
    resp::{RedisWrite, RespType},
};

#[derive(RedisCommand)]
#[redis_command(syntax = "REPLCONF args..")]
pub struct Replconf {
    args: Vec<Bytes>,
}

#[async_trait]
impl AsyncCommand for Replconf {
    async fn run_command(
        &self,
        _ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::server::RedisError> {
        let mut args = self.args.iter();
        match args.next() {
            Some(arg) => match arg.to_ascii_lowercase().as_slice() {
                b"listening-port" => {
                    if args.next().is_some() {
                        RespType::simple_string("OK").write_to_buf(buf);
                        Ok(())
                    } else {
                        Err(CommandError::new(
                            self.syntax(),
                            StreamParseError::Expected("port".into(), "empty".into()).into(),
                        )
                        .into())
                    }
                }
                b"capa" => {
                    if let Some(next) = args.next() {
                        if next.to_ascii_lowercase().as_slice() == b"psync2" {
                            RespType::simple_string("OK").write_to_buf(buf);
                            Ok(())
                        } else {
                            Err(CommandError::new(
                                self.syntax(),
                                StreamParseError::Expected(
                                    "psync2".into(),
                                    String::from_utf8_lossy(next).into(),
                                )
                                .into(),
                            )
                            .into())
                        }
                    } else {
                        Err(
                            CommandError::new(self.syntax(), StreamParseError::EmptyArg.into())
                                .into(),
                        )
                    }
                }
                _ => todo!(),
            },
            None => Err(CommandError::new(self.syntax(), StreamParseError::EmptyArg.into()).into()),
        }
    }
}

#[derive(RedisCommand)]
#[redis_command(syntax = "PSYNC replicationid offset")]
#[allow(dead_code)]
pub struct Psync {
    repl_id: Bytes,
    offset: Bytes,
}

#[async_trait]
impl AsyncCommand for Psync {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::server::RedisError> {
        let info = ctx.replication.read().await;
        RespType::simple_string(format!("FULLRESYNC {} 0", info.replication_id)).write_to_buf(buf);
        let rdb_file = RdbFile::open_file("static/empty.rdb").await?;
        rdb_file.write_to_buf(buf);
        Ok(())
    }
}
