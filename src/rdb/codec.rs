use std::{
    ops::Deref,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use either::Either;
use hashbrown::HashMap;
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, FramedRead};

pub const RDB_KV_STR: u8 = 0;
pub const RDB_KV_LIST: u8 = 1;

const RDB_CODE_EOF: u8 = 0xFF;
const RDB_CODE_SELECT_DB: u8 = 0xFE;
const RDB_CODE_EXPIRY: u8 = 0xFD;
const RDB_CODE_EXPIRY_MS: u8 = 0xFC;
const RDB_CODE_RESIZE_DB: u8 = 0xFB;
const RDB_CODE_AUX: u8 = 0xFA;

#[allow(unused)]
pub struct RdbFile {
    // Starts with REDIS in ascii i.e.
    // 52 45 44 49 53
    pub version: usize,
    metadata: MetadataSection,
    databases: DatabaseSection,
    checksum: Bytes,
}

impl RdbFile {
    pub fn databases(&self) -> &[Vec<RdbKeyValue>] {
        &self.databases.0
    }
}

impl RdbFile {
    pub async fn read_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let file = tokio::fs::File::open(path).await?;
        let mut framed_read = FramedRead::new(file, RdbCodec::default());
        loop {
            if let Some(rdb) = framed_read.next().await {
                let rdb = rdb?;
                return Ok(rdb);
            }
        }
    }
}

#[derive(Default)]
pub struct RdbCodec {
    cursor: usize,
}

impl Decoder for RdbCodec {
    type Item = RdbFile;

    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.cursor = 0;
        if src[self.cursor..5].to_ascii_lowercase().as_slice() != b"redis" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "missing Magic String 'REDIS'",
            )
            .into());
        }
        self.cursor += 5;

        // let version: usize = String::from_utf8_lossy(&src[self.cursor..(self.cursor)])
        //     .parse()
        //     .map_err(|err| {
        //         std::io::Error::new(
        //             std::io::ErrorKind::InvalidInput,
        //             format!("Expected ascii number {err}"),
        //         )
        //     })?;
        self.cursor += 4;
        let metadata = if src[self.cursor] == RDB_CODE_AUX {
            self.cursor += 1;
            let mut map = HashMap::new();
            loop {
                let backup = self.cursor;

                if src.get(self.cursor) == Some(&RDB_CODE_SELECT_DB) {
                    break;
                }
                if let Some(key) = RdbLenStr::decode_stream(self, src)? {
                    match key.to_ascii_lowercase().as_slice() {
                        b"redis-ver" => {
                            let Some(value) = RdbLenStr::decode_stream(self, src)? else {
                                return Ok(None);
                            };
                            map.insert(key, value);
                        }
                        b"redis-bits" => {
                            let Some(_) = LenEncoding::decode_stream(self, src)? else {
                                return Ok(None);
                            };
                        }
                        _ => {
                            self.cursor = backup;
                            break;
                        }
                    }
                }
            }
            map
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "missing AUX OpCode",
            )
            .into());
        };

        let mut databases = DatabaseSection(vec![]);

        loop {
            if src[self.cursor] == RDB_CODE_SELECT_DB {
                self.cursor += 1;
                // IDX SIZE
                if LenEncoding::decode_stream(self, src)?.is_some() {
                    self.cursor += 1;
                    if src[self.cursor] == RDB_CODE_RESIZE_DB {
                        self.cursor += 1;
                        // Hash table size
                        if let Some(size) = LenEncoding::decode_stream(self, src)? {
                            // Expiry size
                            if LenEncoding::decode_stream(self, src)?.is_some() {
                                let mut keys = vec![];
                                for _ in 0..size {
                                    if let Some(kv) = RdbKeyValue::decode_stream(self, src)? {
                                        keys.push(kv);
                                    } else {
                                        return Ok(None);
                                    }
                                }
                                databases.add_database(keys);
                            } else {
                                return Ok(None);
                            }
                        } else {
                            return Ok(None);
                        }
                    } else {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "missing the Resize DB Op Code",
                        )
                        .into());
                    }
                } else {
                    return Ok(None);
                }
            } else {
                break;
            }
        }

        if let Some(code) = src.get(self.cursor)
            && code == &RDB_CODE_EOF
        {
            self.cursor += 1;
            if let Some(checksum) = src.get(self.cursor..(self.cursor + 8)) {
                let out = Ok(Some(RdbFile {
                    version: 3,
                    metadata: MetadataSection {
                        attributes: metadata,
                    },
                    databases,
                    checksum: Bytes::copy_from_slice(checksum),
                }));
                self.cursor += 8;
                src.advance(self.cursor);
                out
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}
pub struct MetadataSection {
    #[allow(unused)]
    attributes: HashMap<Bytes, Bytes>,
}

#[derive(Default)]
pub struct DatabaseSection(Vec<Vec<RdbKeyValue>>);

impl DatabaseSection {
    pub fn add_database(&mut self, values: Vec<RdbKeyValue>) {
        self.0.push(values);
    }
}

impl Encoder<DatabaseSection> for RdbCodec {
    type Error = std::io::Error;
    fn encode(&mut self, item: DatabaseSection, dst: &mut BytesMut) -> Result<(), Self::Error> {
        for (idx, db) in item.0.iter().enumerate() {
            dst.put_u8(RDB_CODE_SELECT_DB);
            Encoder::encode(self, idx, dst)?;
            dst.put_u8(RDB_CODE_RESIZE_DB);
            Encoder::encode(self, db.len(), dst)?;
            let expiry_count = db.iter().filter(|value| value.expiry.is_some()).count();
            Encoder::encode(self, expiry_count, dst)?;
            for value in db {
                Encoder::encode(self, value.to_owned(), dst)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct RdbKeyValue {
    key: RdbLenStr,
    value: RdbLenStr,
    kind: u8,
    /// If the key is expiring it's either
    /// expressed as milliseconds as an 8-byte uint in little-endian
    /// or
    /// as seconds as a 4-byte uint in little endian
    expiry: Option<Either<u64, u32>>,
}

impl RdbKeyValue {
    pub fn key(&self) -> &Bytes {
        &self.key.0
    }
    pub fn value(&self) -> &Bytes {
        &self.value.0
    }
    pub fn expiry(&self) -> Option<SystemTime> {
        if let Some(expiry) = self.expiry {
            match expiry {
                Either::Left(milliseconds) => {
                    Some(UNIX_EPOCH + Duration::from_millis(milliseconds))
                }
                Either::Right(seconds) => Some(UNIX_EPOCH + Duration::from_secs(seconds as u64)),
            }
        } else {
            None
        }
    }
}

impl RdbKeyValue {
    fn decode_stream(
        codec: &mut RdbCodec,
        src: &mut BytesMut,
    ) -> Result<Option<Self>, std::io::Error> {
        if let Some(code) = src.get(codec.cursor) {
            match *code {
                RDB_KV_STR => {
                    codec.cursor += 1;
                    let Some(key) = RdbLenStr::decode_stream(codec, src)? else {
                        return Ok(None);
                    };
                    let Some(value) = RdbLenStr::decode_stream(codec, src)? else {
                        return Ok(None);
                    };
                    let expiry: Option<Either<u64, u32>> = if let Some(code) = src.get(codec.cursor)
                    {
                        if code == &RDB_CODE_EXPIRY {
                            codec.cursor += 1;
                            let Some(seconds) = src.get(codec.cursor..(codec.cursor + 4)) else {
                                return Ok(None);
                            };
                            let out = Some(Either::Right(u32::from_le_bytes(
                                seconds.try_into().unwrap(),
                            )));
                            codec.cursor += 4;
                            out
                        } else if code == &RDB_CODE_EXPIRY_MS {
                            codec.cursor += 1;
                            let Some(milliseconds) = src.get(codec.cursor..(codec.cursor + 8))
                            else {
                                return Ok(None);
                            };
                            let out = Some(Either::Left(u64::from_le_bytes(
                                milliseconds.try_into().unwrap(),
                            )));
                            codec.cursor += 8;
                            out
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    Ok(Some(RdbKeyValue {
                        key: key.into(),
                        value: value.into(),
                        kind: RDB_KV_STR,
                        expiry,
                    }))
                }
                RDB_KV_LIST => todo!(),
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}

impl Encoder<RdbKeyValue> for RdbCodec {
    type Error = std::io::Error;
    fn encode(&mut self, item: RdbKeyValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.put_u8(item.kind);
        Encoder::encode(self, item.key, dst)?;
        Encoder::encode(self, item.value, dst)?;
        if let Some(expiry) = item.expiry {
            match expiry {
                Either::Left(milliseconds) => {
                    dst.put_u64_le(milliseconds);
                }
                Either::Right(seconds) => {
                    dst.put_u32_le(seconds);
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct RdbLenStr(Bytes);

impl Deref for RdbLenStr {
    type Target = Bytes;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl RdbLenStr {
    fn decode_stream(
        codec: &mut RdbCodec,
        src: &mut BytesMut,
    ) -> Result<Option<Bytes>, std::io::Error> {
        if let Some(len) = LenEncoding::decode_stream(codec, src)? {
            let Some(out) = &src.get(codec.cursor..(codec.cursor + len)) else {
                return Ok(None);
            };
            codec.cursor += len;

            Ok(Some(Bytes::copy_from_slice(out)))
        } else {
            Ok(None)
        }
    }
}

impl From<Bytes> for RdbLenStr {
    fn from(value: Bytes) -> Self {
        Self(value)
    }
}

impl Encoder<RdbLenStr> for RdbCodec {
    type Error = std::io::Error;
    fn encode(&mut self, item: RdbLenStr, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Encoder::encode(self, item.0.len(), dst)?;
        dst.put(item.0.clone());
        Ok(())
    }
}

impl Encoder<i8> for RdbCodec {
    type Error = std::io::Error;
    fn encode(&mut self, item: i8, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let len = 0b1100_0000;
        dst.put_u8(len);
        dst.put_i8(item);
        Ok(())
    }
}
impl Encoder<i16> for RdbCodec {
    type Error = std::io::Error;
    fn encode(&mut self, item: i16, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let len = 0b1100_0001;
        dst.put_u8(len);
        dst.put_i16(item);
        Ok(())
    }
}
impl Encoder<i32> for RdbCodec {
    type Error = std::io::Error;
    fn encode(&mut self, item: i32, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let len = 0b1100_0010;
        dst.put_u8(len);
        dst.put_i32(item);
        Ok(())
    }
}

pub enum LenEncoding {
    BasicLen = 0b00,
    ReadOne = 0b01,
    Discard = 0b10,
    Special = 0b11,
}
impl LenEncoding {
    pub fn check_val(value: u8) -> Self {
        let bits = value >> 6;
        match bits {
            0b00 => Self::BasicLen,
            0b01 => Self::ReadOne,
            0b10 => Self::Discard,
            0b11 => Self::Special,
            _ => unreachable!(),
        }
    }
}

impl LenEncoding {
    pub fn decode_stream(
        codec: &mut RdbCodec,
        src: &mut BytesMut,
    ) -> Result<Option<usize>, std::io::Error> {
        if let Some(first) = src.get(codec.cursor) {
            match LenEncoding::check_val(*first) {
                LenEncoding::BasicLen => {
                    let len = src[codec.cursor] << 2;
                    let len = len >> 2;
                    codec.cursor += 1;
                    Ok(Some(len as usize))
                }
                LenEncoding::ReadOne => {
                    let Some(len) = src.get(codec.cursor..(codec.cursor + 2)) else {
                        return Ok(None);
                    };
                    let first_bit = len[0] << 2;
                    let first_bit = first_bit >> 2;
                    let out = usize::from_be_bytes(vec![first_bit, len[1]].try_into().unwrap());
                    codec.cursor += 2;
                    Ok(Some(out))
                }
                LenEncoding::Discard => {
                    codec.cursor += 1;
                    let Some(len) = src.get(codec.cursor..(codec.cursor + 4)) else {
                        return Ok(None);
                    };
                    let out = usize::from_be_bytes(len.try_into().unwrap());
                    codec.cursor += 4;
                    Ok(Some(out))
                }
                LenEncoding::Special => {
                    let len = src[codec.cursor] << 2;
                    let len = len >> 2;
                    println!("LEN: {len:#010b}");
                    codec.cursor += 1;
                    match len {
                        0 => {
                            if let Some(first) = src.get(codec.cursor) {
                                let out = Ok(Some(*first as usize));
                                codec.cursor += 1;
                                out
                            } else {
                                Ok(None)
                            }
                        }
                        1 => {
                            let Some(value) = src.get(codec.cursor..(codec.cursor + 2)) else {
                                return Ok(None);
                            };
                            let out = usize::from_be_bytes(value.try_into().unwrap());
                            codec.cursor += 2;
                            Ok(Some(out))
                        }
                        2 => {
                            let Some(len) = src.get(codec.cursor..(codec.cursor + 4)) else {
                                return Ok(None);
                            };
                            let out = usize::from_be_bytes(len.try_into().unwrap());
                            codec.cursor += 4;
                            Ok(Some(out))
                        }
                        _ => Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!("invalid value for Integer as string: {len}"),
                        )),
                    }
                }
            }
        } else {
            Ok(None)
        }
    }
}

impl Encoder<usize> for RdbCodec {
    type Error = std::io::Error;
    fn encode(&mut self, item: usize, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if item <= 63 {
            let value = item as u8 & 0b0011_1111;
            dst.put_u8(value);
        } else if item <= 16383 {
            let mut value = (item as u16).to_be_bytes();
            value[0] &= 0b0011_1111;
            value[0] |= 0b0100_0000;
            dst.put_slice(&value);
        } else {
            let len = 0b1100_0000;
            let value = (item as u32).to_be_bytes();
            dst.put_u8(len);
            dst.put(value.as_slice());
        }
        Ok(())
    }
}
