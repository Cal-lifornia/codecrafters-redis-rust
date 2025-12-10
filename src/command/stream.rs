use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use either::Either;
use hashbrown::HashMap;
use redis_proc_macros::RedisCommand;
use tokio::time::Instant;

use crate::command::macros::Symbol;
use crate::command::{SymbolBlock, SymbolStreams};
use crate::database::StreamQuery;
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

#[derive(RedisCommand)]
#[redis_command(
    syntax = "XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]",
    impl_parse
)]
pub struct Xread {
    timeout: Option<u64>,
    queries: Vec<StreamQuery>,
}

impl ParseStream for Xread {
    fn parse_stream(
        stream: &mut crate::redis_stream::RedisStream,
    ) -> Result<Self, crate::redis_stream::StreamParseError> {
        let timeout = {
            if stream.parse::<Option<SymbolBlock>>()?.is_some() {
                Some(stream.parse::<u64>()?)
            } else {
                None
            }
        };
        stream.parse::<SymbolStreams>()?;
        let all_queries = stream.parse::<Vec<Bytes>>()?;
        let (keys, ids) = all_queries.split_at(all_queries.len() / 2);
        let mut res_ids = vec![];
        for id in ids {
            res_ids.push(Id::try_from_str(str::from_utf8(id).expect("valid utf-8"))?);
        }
        let queries = keys
            .iter()
            .zip(res_ids.iter())
            .map(|(key, id)| StreamQuery {
                key: key.clone(),
                id: *id,
            })
            .collect();
        Ok(Self { timeout, queries })
    }
}

#[async_trait]
impl AsyncCommand for Xread {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError> {
        if let Some(timeout) = self.timeout {
            let timeout = if timeout == 0 {
                None
            } else {
                Some(Instant::now() + Duration::from_millis(timeout))
            };
            let mut receiver = ctx.db.block_read_stream(&self.queries, timeout).await;
            let result = if let Some(timeout) = timeout {
                match tokio::time::timeout_at(timeout, receiver.recv()).await {
                    Ok(result) => result,
                    Err(_) => {
                        NullArray.write_to_buf(buf);
                        return Ok(());
                    }
                }
            } else {
                receiver.recv().await
            };

            if let Some(result) = result {
                if result.is_empty() {
                    NullArray.write_to_buf(buf);
                } else {
                    result.write_to_buf(buf);
                }
            } else {
                let message = "ERR Channel closed";
                eprintln!("{message}");
                RespType::simple_error(message.into()).write_to_buf(buf);
                return Ok(());
            }
            receiver.close();
        } else {
            let results = ctx.db.read_stream(&self.queries).await;
            results.write_to_buf(buf);
        }

        Ok(())
    }
}
