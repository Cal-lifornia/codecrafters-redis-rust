use bytes::{Buf, BufMut, Bytes, BytesMut};
use either::Either;
use hashbrown::HashMap;
use tokio_util::codec::{Decoder, Encoder};

pub struct RdbFile {
    contents: Bytes,
    // Starts with REDIS in ascii i.e.
    // 52 45 44 49 53
    pub version: usize,
    metadata: MetadataSection,
    databases: DatabaseSection,
    checksum: Bytes,
}
pub struct RdbCodec {}

impl Decoder for RdbCodec {
    type Item = RdbFile;

    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src[0..5].to_ascii_lowercase().as_slice() != b"redis" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "missing Magic String 'REDIS'",
            ));
        }
        src.advance(5);
        let version: usize = String::from_utf8_lossy(&src[0..4]).parse().map_err(|err| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "Expected ascii number")
        })?;
        src.advance(4);
        let metadata = if src[0] == RdbOpCode::Aux as u8 {
            let mut map = HashMap::new();
            let Some(key) = RdbLenStr::decode_stream(src)? else {
                return Ok(None);
            };
            let Some(value) = RdbLenStr::decode_stream(src)? else {
                return Ok(None);
            };
            map.insert(key, value);
            map
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "missing AUX OpCode",
            ));
        };

        let mut databases = vec![];

        loop {
            if src[0] == RdbOpCode::SelectDB as u8 {
                src.advance(1);
                if LenEncoding::decode_size(src)?.is_some() {
                } else {
                    return Ok(None);
                }
            } else {
                break;
            }
        }
    }
}

pub enum RdbOpCode {
    Eof = 0xFF,
    // AKA Database section
    SelectDB = 0xFE,
    ExpireTime = 0xFD,
    ExpireTimeMs = 0xFC,
    ResizeDB = 0xFB,
    // Aka Metadata
    Aux = 0xFA,
}

pub struct MetadataSection {
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
            dst.put_u8(RdbOpCode::SelectDB as u8);
            Encoder::encode(self, idx, dst)?;
            dst.put_u8(RdbOpCode::ResizeDB as u8);
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
    kind: RdbKVKind,
    /// If the key is expiring it's either
    /// expressed as milliseconds as an 8-byte uint in little-endian
    /// or
    /// as seconds as a 4-byte uint in little endian
    expiry: Option<Either<u64, u32>>,
}

impl Encoder<RdbKeyValue> for RdbCodec {
    type Error = std::io::Error;
    fn encode(&mut self, item: RdbKeyValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.put_u8(item.kind as u8);
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

#[derive(Clone, Copy)]
pub enum RdbKVKind {
    String = 0,
    List = 1,
}

#[derive(Clone)]
pub struct RdbLenStr(Bytes);

impl RdbLenStr {
    pub fn empty() -> Self {
        Self(Bytes::new())
    }
}

impl RdbLenStr {
    fn decode_stream(src: &mut BytesMut) -> Result<Option<Bytes>, std::io::Error> {
        if let Some(len) = LenEncoding::decode_size(src)? {
            let out = Ok(Some(Bytes::copy_from_slice(&src[0..len])));
            src.advance(len);

            out
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

impl RdbLenStr {
    pub fn contents(&self) -> &Bytes {
        &self.0
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
    pub fn decode_size(src: &mut BytesMut) -> Result<Option<usize>, std::io::Error> {
        if let Some(first) = src.get(0) {
            match LenEncoding::check_val(*first) {
                LenEncoding::BasicLen => {
                    let len = src.get(0).unwrap() & 0b0011_1111;
                    src.advance(1);
                    Ok(Some(len as usize))
                }
                LenEncoding::ReadOne => {
                    let Some(len) = src.get(0..2) else {
                        return Ok(None);
                    };
                    let first_bit = len[0] & 0b0011_1111;
                    let out = usize::from_be_bytes(vec![first_bit, len[1]].try_into().unwrap());
                    src.advance(2);
                    Ok(Some(out as usize))
                }
                LenEncoding::Discard => {
                    src.advance(1);
                    let Some(len) = src.get(0..4) else {
                        return Ok(None);
                    };
                    let out = usize::from_be_bytes(len.try_into().unwrap());
                    src.advance(4);
                    Ok(Some(out as usize))
                }
                LenEncoding::Special => todo!(),
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
