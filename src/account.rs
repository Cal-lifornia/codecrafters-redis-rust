use std::fmt::Display;

use hashbrown::HashSet;

#[derive(Debug, Clone)]
pub struct Account {
    pub username: String,
    pub flags: HashSet<AccountFlag>,
    pub passwords: Vec<String>,
}

impl Default for Account {
    fn default() -> Self {
        Self {
            username: "default".into(),
            flags: HashSet::from([AccountFlag::NoPass]),
            passwords: vec![],
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum AccountFlag {
    NoPass,
}

impl From<AccountFlag> for &[u8] {
    fn from(value: AccountFlag) -> Self {
        match value {
            AccountFlag::NoPass => b"nopass",
        }
    }
}

impl Display for AccountFlag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let out = match self {
            AccountFlag::NoPass => "nopass",
        };
        write!(f, "{out}")
    }
}
