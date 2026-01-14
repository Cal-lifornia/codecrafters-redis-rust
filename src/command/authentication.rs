use async_trait::async_trait;
use bytes::Bytes;
use redis_proc_macros::RedisCommand;

use crate::{
    account::{AccountDB, AccountFlag},
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
    ) -> Result<(), crate::redis::RedisError> {
        let mut args_iter = self.args.iter();
        if let Some(first) = args_iter.next() {
            match first.to_ascii_lowercase().as_slice() {
                b"whoami" => {
                    RespType::bulk_string(ctx.accounts.read().await.whoami()).write_to_buf(buf);
                }
                b"getuser" => {
                    if let Some(account) = &ctx.accounts.read().await.get_signed_in() {
                        RespType::Array(vec![
                            RespType::bulk_string("flags"),
                            RespType::bulk_string_array(account.flags.iter()),
                            RespType::bulk_string("passwords"),
                            RespType::bulk_string_array(account.passwords.iter()),
                        ])
                        .write_to_buf(buf);
                    }
                }
                b"setuser" => {
                    if let Some(username) = args_iter.next() {
                        let mut accounts = ctx.accounts.write().await;
                        let username_str = String::from_utf8_lossy(username);
                        if let Some(account) = accounts.get_mut(username_str.to_string())
                            && let Some(password) = args_iter.next()
                            && password.starts_with(b">")
                        {
                            let hashed_pass = AccountDB::hash_pass(&password[1..]);
                            dbg!(&hashed_pass);
                            account.flags.remove(&AccountFlag::NoPass);
                            account.passwords.push(hashed_pass);
                            RespType::simple_string("OK").write_to_buf(buf);
                        }
                    }
                }
                _ => {
                    return Err(crate::redis::RedisError::Other(
                        "invalid argument".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(RedisCommand)]
#[redis_command(syntax = "AUTH username password")]
pub struct Auth {
    username: Bytes,
    password: Bytes,
}

#[async_trait]
impl AsyncCommand for Auth {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::redis::RedisError> {
        let username_str = String::from_utf8_lossy(&self.username);
        ctx.accounts
            .write()
            .await
            .auth(&username_str.to_string(), &self.password)?;
        RespType::simple_string("OK").write_to_buf(buf);
        Ok(())
    }
}
