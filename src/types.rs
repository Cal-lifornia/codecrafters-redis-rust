use std::fmt::Display;

use thiserror::Error;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct EntryId {
    pub ms_time: usize,
    pub sequence: usize,
}

impl EntryId {
    fn new(ms_time: usize, sequence: usize) -> Result<Self, EntryParseError> {
        if ms_time == 0 && sequence == 0 {
            Err(EntryParseError::TooSmall)
        } else {
            Ok(Self { ms_time, sequence })
        }
    }
    pub fn new_or_wildcard_from_string(value: String) -> Result<(Self, bool), EntryParseError> {
        let (ms_time, sequence) = match value.split_once("-") {
            Some((ms_time, sequence)) => (ms_time, sequence),
            None => {
                return Err(EntryParseError::MissingCharacter('-'));
            }
        };
        let mut wildcard = false;

        let sequence = if sequence == "*" {
            wildcard = true;
            1
        } else {
            sequence.parse::<usize>()?
        };

        Ok((
            Self {
                ms_time: ms_time.parse::<usize>()?,
                sequence,
            },
            wildcard,
        ))
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
    type Error = EntryParseError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let (ms_time, sequence) = match value.split_once("-") {
            Some((ms_time, sequence)) => (ms_time, sequence),
            None => {
                return Err(EntryParseError::MissingCharacter('-'));
            }
        };

        Self::new(ms_time.parse::<usize>()?, sequence.parse::<usize>()?)
    }
}

#[derive(Debug, Error)]
pub enum EntryParseError {
    #[error("error parsing type to number")]
    NumParseError(#[from] std::num::ParseIntError),
    #[error("missing character {0}")]
    MissingCharacter(char),
    #[error("The ID specified in XADD must be greater than 0-0")]
    TooSmall,
}
