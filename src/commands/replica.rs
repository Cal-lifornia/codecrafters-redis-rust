use bytes::{BufMut, BytesMut};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::broadcast,
};

use crate::{
    resp::{self, Resp},
    types::{Context, Replica},
};

use super::CommandError;

pub async fn replconf_cmd(ctx: &mut Context, args: &[String]) -> Result<(), CommandError>
where
{
    if args.len() == 2 {
        ctx.out
            .write()
            .await
            .write_all(&Resp::SimpleString("OK".to_string()).to_bytes())
            .await?;
    } else {
        ctx.out
            .write()
            .await
            .write_all(
                &Resp::simple_error(CommandError::WrongNumArgs("replconf".into())).to_bytes(),
            )
            .await?;
    }
    Ok(())
}

pub async fn psync_cmd(ctx: &mut Context, args: &[String]) -> Result<(), CommandError>
where
{
    if args.len() > 1 {
        let (repl_id, offset) = {
            let info = ctx.info.read().await;
            (
                info.replication.replication_id.clone(),
                info.replication.offset,
            )
        };
        ctx.out
            .write()
            .await
            .write_all(
                &Resp::SimpleString(format!("FULLRESYNC {} {}", &repl_id, &offset.to_string()))
                    .to_bytes(),
            )
            .await?;
        let binary_rdb = std::fs::read("./static/empty.rdb").unwrap();
        let mut buf = BytesMut::new();
        buf.put_u8(b'$');
        buf.put(binary_rdb.len().to_string().as_bytes());
        buf.put_slice(&[resp::CR, resp::LF]);
        buf.put_slice(&binary_rdb);
        ctx.out.write().await.write_all(&buf).await?;

        ctx.replicas.write().await.push(Replica {
            replica: ctx.out.clone(),
        });
    } else {
        ctx.out
            .write()
            .await
            .write_all(&Resp::simple_error(CommandError::WrongNumArgs("psync".into())).to_bytes())
            .await?;
    }
    Ok(())
}

pub async fn write_to_replicas(ctx: &Context, input: &[Resp]) -> Result<(), CommandError> {
    for Replica { replica } in ctx.replicas.write().await.iter_mut() {
        replica
            .write()
            .await
            .write_all(&Resp::Array(input.to_vec()).to_bytes())
            .await?;
    }
    Ok(())
}
