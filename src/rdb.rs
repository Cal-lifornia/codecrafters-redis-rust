use std::path::Path;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncBufRead, AsyncReadExt};

use crate::resp::RedisWrite;

pub struct RdbFile {
    contents: Bytes,
}

impl RdbFile {
    pub async fn open_file(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut buf = vec![];
        file.read_to_end(&mut buf).await?;
        Ok(Self {
            contents: Bytes::from(buf),
        })
    }
    pub async fn read_from_redis_stream<R: AsyncBufRead + Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        use tokio::io::AsyncBufReadExt;

        let mut file_size = String::new();
        let _ = reader.read_line(&mut file_size).await;
        // println!("file_size: {file_size}");
        let size = file_size
            .trim_start_matches("$")
            .trim_end()
            .parse::<usize>()
            .map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("should have a valid file size; {err}"),
                )
            })?;
        // println!("slave: read RDB with size {size}");
        let mut buf = BytesMut::with_capacity(size);
        buf.resize(size, 0);
        let _ = reader.read_exact(&mut buf).await?;
        Ok(Self {
            contents: buf.into(),
        })
    }
}

impl RedisWrite for RdbFile {
    fn write_to_buf(&self, buf: &mut bytes::BytesMut) {
        let len = self.contents.len();
        buf.put_u8(b'$');
        buf.put_slice(format!("{len}").as_bytes());
        buf.put_slice(b"\r\n");
        buf.put_slice(&self.contents);
    }
}
