use std::{fmt::Display, sync::Arc};

use thiserror::Error;
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::{mpsc, Mutex},
};

use crate::{
    commands::{parse_array_command, CommandError, RedisCommand},
    resp::Resp,
};

pub struct Context<Writer: AsyncWrite + Unpin> {
    pub out: Writer,
    pub db_sender: mpsc::Sender<RedisCommand>,
    pub queued: Arc<Mutex<bool>>,
    pub queue_list: Arc<Mutex<Vec<Vec<Resp>>>>,
    // pub transaction_tx: mpsc::Sender<bool>,
    // transaction_rx: mpsc::Receiver<bool>,
}

impl<Writer: AsyncWrite + Unpin> Context<Writer> {
    pub fn new(
        out: Writer,
        db_sender: mpsc::Sender<RedisCommand>,
        queued: Arc<Mutex<bool>>,
        queue_list: Arc<Mutex<Vec<Vec<Resp>>>>,
    ) -> Self {
        // let (transaction_tx, transaction_rx) = mpsc::channel(4);
        Self {
            out,
            db_sender,
            queued,
            queue_list,
            // transaction_tx,
            // transaction_rx,
        }
    }
    pub async fn handle_transactions(&mut self) -> Result<(), CommandError> {
        let queue_list = {
            let locked_queue_list = self.queue_list.lock().await;
            locked_queue_list.clone()
        };
        if queue_list.is_empty() {
            self.out.write_all(&Resp::Array(vec![]).to_bytes()).await?;
            self.out
                .write_all(&Resp::simple_error("EXEC without MULTI").to_bytes())
                .await?;
            return Ok(());
        }

        for input in queue_list {
            let result = Box::pin(parse_array_command(input, self));
            result.await?;
        }

        {
            let mut locked_queue_list = self.queue_list.lock().await;
            locked_queue_list.clear();
            let mut queued = self.queued.lock().await;
            *queued = false;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct EntryId {
    pub ms_time: usize,
    pub sequence: usize,
}

impl EntryId {
    fn new(ms_time: usize, sequence: usize) -> Self {
        Self { ms_time, sequence }
    }
    pub fn new_or_wildcard_from_string(value: String) -> Result<(Self, bool), EntryIdParseErrore> {
        let (ms_time, sequence) = match value.split_once("-") {
            Some((ms_time, sequence)) => (ms_time, sequence),
            None => {
                return Err(EntryIdParseErrore::MissingCharacter('-'));
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
    type Error = EntryIdParseErrore;
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

#[derive(Debug, Error)]
pub enum EntryIdParseErrore {
    #[error("error parsing type to number")]
    NumParseError(#[from] std::num::ParseIntError),
    #[error("missing character {0}")]
    MissingCharacter(char),
}
