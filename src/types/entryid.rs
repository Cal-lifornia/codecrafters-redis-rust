use std::{fmt::Display, io::Read};

use bytes::{Buf, Bytes};
use thiserror::Error;
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct EntryId {
    pub ms_time: usize,
    pub sequence: usize,
}

impl EntryId {
    fn new(ms_time: usize, sequence: usize) -> Self {
        Self { ms_time, sequence }
    }
    pub fn new_or_wildcard_from_bytes(value: Bytes) -> Result<(Self, bool), EntryIdParseError> {
        let mut buf = String::new();
        value.reader().read_to_string(&mut buf).unwrap();

        let (ms_time, sequence) = match buf.split_once("-") {
            Some((ms_time, sequence)) => (ms_time, sequence),
            None => {
                return Err(EntryIdParseError::MissingCharacter('-'));
            }
        };
        let mut wildcard = false;

        let sequence = if sequence == "*" {
            wildcard = true;
            1
        } else {
            sequence.parse::<usize>()?
        };

        Ok((Self::new(ms_time.parse::<usize>()?, sequence), wildcard))
    }
}

impl Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms_time, self.sequence)
    }
}

impl Ord for EntryId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ms_time
            .cmp(&other.ms_time)
            .then(self.sequence.cmp(&other.sequence))
    }
}

impl PartialOrd for EntryId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<EntryId> for String {
    fn from(value: EntryId) -> Self {
        format!("{value}")
    }
}

impl TryFrom<String> for EntryId {
    type Error = EntryIdParseError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.split_once("-") {
            Some((ms_time, sequence)) => Ok(Self::new(
                ms_time.parse::<usize>()?,
                sequence.parse::<usize>()?,
            )),

            None => Ok(Self::new(value.parse::<usize>()?, 0)),
        }
    }
}

impl TryFrom<Bytes> for EntryId {
    type Error = EntryIdParseError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let mut buf = String::new();
        value.reader().read_to_string(&mut buf).unwrap();

        Self::try_from(buf)
    }
}

#[derive(Debug, Error)]
pub enum EntryIdParseError {
    #[error("error parsing type to number")]
    NumParseError(#[from] std::num::ParseIntError),
    #[error("missing character {0}")]
    MissingCharacter(char),
}
