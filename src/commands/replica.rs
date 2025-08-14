use bytes::{BufMut, BytesMut};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::broadcast,
};

use crate::{
    resp::{self, Resp},
    types::Context,
};

use super::CommandError;

pub async fn replconf_cmd<Writer>(
    ctx: &mut Context<Writer>,
    args: &[String],
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    if args.len() == 2 {
        ctx.out
            .write_all(&Resp::SimpleString("OK".to_string()).to_bytes())
            .await?;
    } else {
        ctx.out
            .write_all(
                &Resp::simple_error(CommandError::WrongNumArgs("replconf".into())).to_bytes(),
            )
            .await?;
    }
    Ok(())
}

pub async fn psync_cmd<Writer>(
    ctx: &mut Context<Writer>,
    args: &[String],
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
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
        ctx.out.write_all(&buf).await?;
    } else {
        ctx.out
            .write_all(&Resp::simple_error(CommandError::WrongNumArgs("psync".into())).to_bytes())
            .await?;
    }
    let receiver = ctx.cmd_broadcaster.subscribe();
    println!("entering downstream command");
    if let Err(err) = downstream_command(ctx, receiver).await {
        eprintln!("ran into error broadcasting commands {err}");

        return Err(err);
    }
    Ok(())
}

pub async fn downstream_command<Writer>(
    ctx: &mut Context<Writer>,
    mut command_streamer: broadcast::Receiver<Vec<Resp>>,
) -> Result<(), CommandError>
where
    Writer: AsyncWrite + Unpin,
{
    loop {
        let commands = command_streamer.recv().await?;
        println!("sent {commands:#?} to stream");
        ctx.out.write_all(&Resp::Array(commands).to_bytes()).await?;
    }
}

pub async fn send_command(
    sender: broadcast::Sender<Vec<Resp>>,
    command: &str,
    args: &[String],
) -> Result<(), CommandError> {
    let mut input: Vec<Resp> = vec![Resp::BulkString(command.to_string())];
    for arg in args {
        input.push(Resp::BulkString(arg.to_string()));
    }

    if sender.send(input).is_err() {
        eprintln!("failed to broadcast command {command}");
    }
    Ok(())
}
