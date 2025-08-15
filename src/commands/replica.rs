use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::AsyncWriteExt;

use crate::{
    commands::CommandResult,
    resp::{self, Resp},
    types::{Context, Replica},
};

use super::CommandError;

pub async fn replconf_cmd(args: &[Bytes]) -> CommandResult {
    if args.len() == 2 {
        Ok(Resp::SimpleString(Bytes::from_static(b"OK")))
    } else {
        Err(CommandError::WrongNumArgs("replconf".to_string()))
    }
}

pub async fn psync_cmd(ctx: &Context, args: &[Bytes]) -> Result<(), CommandError> {
    if args.len() > 1 {
        let (repl_id, offset) = {
            let info = ctx.info.read().await;
            (
                info.replication.replication_id.clone(),
                info.replication.offset,
            )
        };
        let output = format!("FULLRESYNC {} {}", &repl_id, &offset.to_string());
        ctx.out
            .write()
            .await
            .write_all(&Resp::SimpleString(Bytes::from(output)).to_bytes())
            .await
            .unwrap();
        let binary_rdb = std::fs::read("./static/empty.rdb").unwrap();
        let mut buf = BytesMut::new();
        buf.put_u8(b'$');
        buf.put(binary_rdb.len().to_string().as_bytes());
        buf.put_slice(&[resp::CR, resp::LF]);
        buf.put_slice(&binary_rdb);
        ctx.out.write().await.write_all(&buf).await.unwrap();

        ctx.replicas.write().await.push(Replica {
            replica: ctx.out.clone(),
        });
        Ok(())
    } else {
        Err(CommandError::WrongNumArgs("psync".into()))
    }
}
