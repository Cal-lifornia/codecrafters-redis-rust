use async_trait::async_trait;
use bytes::Bytes;
use either::Either;
use hashbrown::HashMap;
use redis_proc_macros::RedisCommand;

use crate::command::macros::Symbol;
use crate::id::Id;
use crate::redis_stream::ParseStream;
use crate::resp::NullArray;
use crate::{
    command::AsyncCommand,
    id::WildcardID,
    resp::{RedisWrite, RespType},
};

#[derive(RedisCommand)]
#[redis_command(
    syntax = "XADD key [NOMKSTREAM] [KEEPREF | DELREF | ACKED] [<MAXLEN | MINID> [= | ~] threshold
  [LIMIT count]] <* | id> field value [field value ...]"
)]
pub struct Xadd {
    key: Bytes,
    id: WildcardID,
    values: HashMap<Bytes, Bytes>,
}

#[async_trait]
impl AsyncCommand for Xadd {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError> {
        match ctx
            .db
            .add_stream(self.key.clone(), self.id, self.values.clone())
            .await
        {
            Ok(id) => id.write_to_buf(buf),
            Err(err) => RespType::simple_error(err.to_string()).write_to_buf(buf),
        }
        Ok(())
    }
}

#[derive(RedisCommand)]
#[redis_command(syntax = "XRANGE key start end [COUNT count]")]
pub struct Xrange {
    key: Bytes,
    start: Either<Symbol!("-"), XrangeIdInput>,
    end: Either<Symbol!("+"), XrangeIdInput>,
    count: Option<i64>,
}
pub enum XrangeIdInput {
    MsTime(u64),
    Id(Id),
}

impl ParseStream for XrangeIdInput {
    fn parse_stream(
        stream: &mut crate::redis_stream::RedisStream,
    ) -> Result<Self, crate::redis_stream::StreamParseError> {
        if let Some(next) = stream.peek().cloned() {
            if next.contains(&b'-') {
                let wildcard = WildcardID::parse_stream(stream)?;
                if let Some(id) = Id::from_wildcard(wildcard) {
                    Ok(XrangeIdInput::Id(id))
                } else {
                    Err(crate::redis_stream::StreamParseError::Expected(
                        "A valid id".into(),
                        String::from_utf8(next.to_vec()).expect("valid utf-8"),
                    ))
                }
            } else {
                Ok(Self::MsTime(stream.parse::<u64>()?))
            }
        } else {
            Err(crate::redis_stream::StreamParseError::EmptyArg)
        }
    }
}

#[async_trait]
impl AsyncCommand for Xrange {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError> {
        let start = if let Either::Right(start) = &self.start {
            Some(start)
        } else {
            None
        };
        let end = if let Either::Right(end) = &self.end {
            Some(end)
        } else {
            None
        };
        let values = ctx.db.range_stream(&self.key, start, end).await;
        if values.is_empty() {
            NullArray.write_to_buf(buf);
        } else {
            values.write_to_buf(buf);
        }
        Ok(())
    }
}
