use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;
use sha2::{Digest, Sha256};

use crate::{
    account::AccountFlag,
    command::AsyncCommand,
    resp::{RedisWrite, RespType},
};

#[derive(RedisCommand)]
#[redis_command(syntax = "ACL ...")]
pub struct Acl {
    args: Vec<Bytes>,
}

#[async_trait]
impl AsyncCommand for Acl {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::server::RedisError> {
        let mut args_iter = self.args.iter();
        if let Some(first) = args_iter.next() {
            match first.to_ascii_lowercase().as_slice() {
                b"whoami" => {
                    RespType::bulk_string(ctx.account.read().await.username.clone())
                        .write_to_buf(buf);
                }
                b"getuser" => {
                    let account = &ctx.account.read().await;
                    RespType::Array(vec![
                        RespType::bulk_string("flags"),
                        RespType::bulk_string_array(account.flags.iter()),
                        RespType::bulk_string("passwords"),
                        RespType::bulk_string_array(account.passwords.iter()),
                    ])
                    .write_to_buf(buf);
                }
                b"setuser" => {
                    if let Some(username) = args_iter.next() {
                        let mut account = ctx.account.write().await;
                        let username_str = String::from_utf8_lossy(username);
                        if username_str == account.username
                            && let Some(password) = args_iter.next()
                            && password.starts_with(b">")
                        {
                            let hashed_pass = Sha256::digest(&password[1..]);
                            let hash_hex = hashed_pass
                                .iter()
                                .map(|b| format!("{b:02x}"))
                                .collect::<String>();
                            account.flags.remove(&AccountFlag::NoPass);
                            account.passwords.push(hash_hex);
                            RespType::simple_string("OK").write_to_buf(buf);
                        }
                    }
                }
                _ => {
                    return Err(crate::server::RedisError::Other(
                        "invalid argument".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}
