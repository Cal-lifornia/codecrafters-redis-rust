use std::{
    fmt::Display,
    hash::{BuildHasher, Hash},
};

use hashbrown::{DefaultHashBuilder, HashSet, HashTable};
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum AccountError {
    #[error("WRONGPASS invalid username-password pair or user is disabled")]
    IncorrectDetails,
}

pub struct AccountDB {
    signed_in: String,
    db: HashTable<Account>,
    hasher: DefaultHashBuilder,
    // pw_hash: Sha256,
}

impl AccountDB {
    pub fn insert(&mut self, account: Account) {
        self.db
            .insert_unique(self.hasher.hash_one(&account.username), account, |val| {
                self.hasher.hash_one(&val.username)
            });
    }
    pub fn whoami(&self) -> &str {
        &self.signed_in
    }
    pub fn get_signed_in(&self) -> Option<&Account> {
        self.db.find(self.hasher.hash_one(&self.signed_in), |val| {
            val.username == self.signed_in
        })
    }
    pub fn auth(
        &mut self,
        username: &String,
        password: impl AsRef<[u8]>,
    ) -> Result<(), AccountError> {
        let hashed_pass = Self::hash_pass(password);
        dbg!(&hashed_pass);
        if let Some(account) = self.db.find(self.hasher.hash_one(username), |val| {
            if &val.username == username {
                if val.flags.contains(&AccountFlag::NoPass) {
                    true
                } else {
                    tracing::debug!("FOUND ACCOUNT");
                    val.passwords.contains(&hashed_pass)
                }
            } else {
                false
            }
        }) {
            self.signed_in = account.username.clone();
            Ok(())
        } else {
            Err(AccountError::IncorrectDetails)
        }
    }
    pub fn get_mut(&mut self, username: String) -> Option<&mut Account> {
        let hash = self.hasher.hash_one(&username);
        self.db.find_mut(hash, |val| val.username == username)
    }
    pub fn hash_pass(password: impl AsRef<[u8]>) -> String {
        let hashed_pass = Sha256::digest(password);
        hashed_pass.iter().map(|b| format!("{b:02x}")).collect()
    }
}

impl Default for AccountDB {
    fn default() -> Self {
        let db = HashTable::new();
        let mut out = Self {
            signed_in: "default".to_string(),
            db,
            hasher: DefaultHashBuilder::default(),
        };
        let default_account = Account::default();
        out.insert(default_account.clone());
        out
    }
}

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
