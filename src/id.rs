use std::fmt::Display;

#[derive(Clone)]
pub struct WildcardID {
    pub ms_time: Option<usize>,
    pub sequence: Option<usize>,
}

impl WildcardID {
    pub fn try_from_str(value: &str) -> Result<Self, Box<dyn std::error::Error>> {
        if value == "*" {
            Ok(Self {
                ms_time: None,
                sequence: None,
            })
        } else {
            let (ms_time, sequence) = match value.split_once("-") {
                Some((ms_time, sequence)) => (ms_time, sequence),
                None => {
                    return Err("missing character '-'".into());
                }
            };
            let ms_time = Some(ms_time.parse::<usize>()?);
            if sequence == "*" {
                Ok(Self {
                    ms_time,
                    sequence: None,
                })
            } else {
                Ok(Self {
                    ms_time,
                    sequence: Some(sequence.parse::<usize>()?),
                })
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct Id {
    pub ms_time: usize,
    pub sequence: usize,
}

impl Id {
    pub fn from_wildcard(wildcard: WildcardID) -> Option<Self> {
        if let (Some(ms_time), Some(sequence)) = (wildcard.ms_time, wildcard.sequence) {
            Some(Self { ms_time, sequence })
        } else {
            None
        }
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms_time, self.sequence)
    }
}

impl Ord for Id {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ms_time
            .cmp(&other.ms_time)
            .then(self.sequence.cmp(&other.sequence))
    }
}

impl PartialOrd for Id {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
