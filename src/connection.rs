use std::sync::Arc;

use bytes::BytesMut;
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::RwLock,
};
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use crate::{
    command::handle_command,
    context::Context,
    resp::{RedisWrite, RespCodec, RespType},
};

#[derive(Clone)]
pub struct Connection {
    pub reader: Arc<RwLock<FramedRead<BufReader<OwnedReadHalf>, RespCodec>>>,
    pub writer: Arc<RwLock<OwnedWriteHalf>>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();

        Self {
            reader: Arc::new(RwLock::new(FramedRead::new(
                BufReader::new(reader),
                RespCodec::default(),
            ))),
            writer: Arc::new(RwLock::new(writer)),
        }
    }
    pub async fn handle(&self, master_conn: bool, app_data: crate::context::AppData) {
        let signed_in = if master_conn {
            Some(0)
        } else {
            let accounts = app_data.accounts.read().await;
            match accounts.auth(&"default".to_string(), None::<&[u8]>) {
                Ok(id) => Some(id),
                Err(err) => {
                    tracing::info!("invalid signin to default: {err}");
                    None
                }
            }
        };
        let ctx = Context {
            writer: self.writer.clone(),
            transactions: Arc::new(RwLock::new(None)),
            signed_in: Arc::new(RwLock::new(signed_in)),
            master_conn,
            get_ack: Arc::new(RwLock::new(false)),
            app_data,
        };
        let mut reader = self.reader.clone().write_owned().await;
        tokio::spawn(async move {
            let ctx = ctx.clone();
            loop {
                if let Some(result) = reader.next().await {
                    let cmd = match result {
                        Ok(cmd) => cmd,
                        Err(err) => {
                            let mut buf = BytesMut::new();
                            tracing::error!("ERROR {err}");
                            RespType::simple_error(err).write_to_buf(&mut buf);
                            let mut writer = ctx.writer.write().await;
                            writer.write_all(&buf).await.expect("valid read");
                            continue;
                        }
                    };
                    if let Err(err) = handle_command(ctx.clone(), cmd).await {
                        let mut buf = BytesMut::new();
                        tracing::error!("ERROR {err}");
                        RespType::simple_error(err).write_to_buf(&mut buf);
                        let mut writer = ctx.writer.write().await;
                        writer.write_all(&buf).await.expect("valid read");
                        continue;
                    }
                }
            }
        });
    }
}
