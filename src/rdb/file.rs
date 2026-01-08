use std::path::Path;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::AsyncReadExt;

use crate::resp::RedisWrite;

impl RedisWrite for EncodedRdbFile {
    fn write_to_buf(&self, buf: &mut bytes::BytesMut) {
        let len = self.0.len();
        buf.put_u8(b'$');
        buf.put_slice(len.to_string().as_bytes());
        buf.put_slice(b"\r\n");
        buf.put_slice(&self.0);
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct EncodedRdbFile(pub Bytes);
impl EncodedRdbFile {
    pub async fn open_file(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut buf = vec![];
        file.read_to_end(&mut buf).await?;
        Ok(Self(buf.into()))
    }
}
