use std::{fmt::Display, hash::Hash};

use hashbrown::HashSet;
use indexmap::{IndexSet, set::MutableValues};
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum AccountError {
    #[error("WRONGPASS invalid username-password pair or user is disabled")]
    IncorrectDetails,
    // #[error("TAKEN account already exists")]
    // AccountExists,
    #[error("NOAUTH Authentication required")]
    NotAuthenticated,
}

// #[derive(Debug, Clone, Copy)]
// pub struct AccountStatus {
//     signed_in: bool,
//     id: usize,
// }

// impl AccountStatus {
//     pub fn new(signed_in: Option<usize>) -> Self {
//         if let Some(id) = signed_in {
//             Self {
//                 signed_in: true,
//                 id,
//             }
//         } else {
//             Self {
//                 signed_in: false,
//                 id: usize::MAX,
//             }
//         }
//     }
//     pub fn signed_in(&self) -> bool {
//         self.signed_in
//     }
//     pub fn sign_in(&mut self, id: usize) {
//         self.signed_in = true;
//         self.id = id;
//     }
// }

pub struct AccountDB {
    accounts: IndexSet<Account>,
}

impl AccountDB {
    // pub fn insert(&mut self, account: Account) -> Result<(), AccountError> {
    //     if self.accounts.contains(&account) {
    //         Err(AccountError::AccountExists)
    //     } else {
    //         self.accounts.insert(account);
    //         Ok(())
    //     }
    // }
    pub fn whoami(&self, id: usize) -> Option<&String> {
        self.accounts.get_index(id).map(|acc| &acc.username)
    }
    pub fn get_account(&self, id: usize) -> Option<&Account> {
        self.accounts.get_index(id)
    }
    pub fn auth(
        &self,
        username: &String,
        password: Option<impl AsRef<[u8]>>,
    ) -> Result<usize, AccountError> {
        if let Some((idx, account)) = self.accounts.get_full(username) {
            if let Some(password) = password {
                let hashed_pass = Self::hash_pass(password);
                if !account.flags.contains(&AccountFlag::NoPass)
                    && account.passwords.contains(&hashed_pass)
                {
                    Ok(idx)
                } else {
                    Err(AccountError::IncorrectDetails)
                }
            } else if account.flags.contains(&AccountFlag::NoPass) {
                Ok(idx)
            } else {
                Err(AccountError::IncorrectDetails)
            }
        } else {
            Err(AccountError::IncorrectDetails)
        }
    }
    pub fn get_mut(&mut self, username: &String) -> Option<&mut Account> {
        self.accounts.get_full_mut2(username).map(|(_, val)| val)
    }
    pub fn hash_pass(password: impl AsRef<[u8]>) -> String {
        let hashed_pass = Sha256::digest(password);
        hashed_pass.iter().map(|b| format!("{b:02x}")).collect()
    }
}

impl Default for AccountDB {
    fn default() -> Self {
        Self {
            accounts: IndexSet::from([Account::default()]),
        }
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

impl Eq for Account {}

impl PartialEq for Account {
    fn eq(&self, other: &Self) -> bool {
        self.username == other.username
    }
}

impl Hash for Account {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.username.hash(state);
    }
}

// impl hashbrown::Equivalent<String> for Account {
//     fn equivalent(&self, key: &String) -> bool {
//         &self.username == key
//     }
// }

impl hashbrown::Equivalent<Account> for String {
    fn equivalent(&self, key: &Account) -> bool {
        self == &key.username
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
