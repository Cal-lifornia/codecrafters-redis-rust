use std::fmt::Debug;

use crate::{context::Context, redis_stream::StreamParseError};

// pub type CmdAction = Box<dyn Fn(&mut Context) -> BoxFuture<Result<(), std::io::Error>>>;

pub trait Command {
    fn name() -> &'static str;
    fn syntax() -> &'static str;
    fn error(kind: CommandErrorKind) -> CommandError {
        CommandError {
            syntax: Self::syntax(),
            kind,
        }
    }
}

#[async_trait::async_trait]
pub trait AsyncCommand {
    async fn run_command(
        &self,
        ctx: &crate::context::Context,
        buf: &mut bytes::BytesMut,
    ) -> Result<(), crate::command::CommandError>;
}

#[derive(Debug)]
pub struct CommandError {
    syntax: &'static str,
    kind: CommandErrorKind,
}

impl std::error::Error for CommandError {}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.kind)?;
        write!(f, "SYNTAX: {}", self.syntax)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CommandErrorKind {
    #[error("{0}")]
    ArgumentParse(#[from] StreamParseError),
}
