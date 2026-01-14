use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    command::{AsyncCommand, SymbolGet},
    resp::{RedisWrite, RespType},
};

#[derive(RedisCommand)]
#[redis_command(syntax = "CONFIG GET ...")]
pub struct ConfigGet {
    #[allow(unused)]
    get: SymbolGet,
    args: Vec<Bytes>,
}

#[async_trait]
impl AsyncCommand for ConfigGet {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::server::RedisError> {
        for arg in &self.args {
            match arg.to_ascii_lowercase().as_slice() {
                b"dir" => {
                    let res = ctx.config.read().await.dir.clone();
                    RespType::bulk_string_array(["dir", res.unwrap_or("".into()).as_str()].iter())
                        .write_to_buf(buf);
                }
                b"dbfilename" => {
                    let res = ctx.config.read().await.db_file_name.clone();
                    RespType::bulk_string_array(
                        ["dbfilename", res.unwrap_or("".into()).as_str()].iter(),
                    )
                    .write_to_buf(buf);
                }
                _ => {
                    eprintln!("WRONG INPUT FOR CONFIG: {arg:#?}");
                    RespType::simple_error("incorrect argument").write_to_buf(buf);
                }
            }
        }
        Ok(())
    }
}
