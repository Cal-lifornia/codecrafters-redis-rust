use std::time::Duration;

use crate::commands::CommandError;

#[derive(Debug)]
pub struct SetCommandArgs {
    pub key: String,
    pub value: String,
    pub expiry: Option<Duration>,
    pub keep_ttl: bool,
}

impl SetCommandArgs {
    pub fn parse(args: Vec<String>) -> Result<Self, CommandError> {
        match args.len() {
            2 => Ok(Self {
                key: args[0].clone(),
                value: args[1].clone(),
                expiry: None,
                keep_ttl: false,
            }),
            3 | 4 => {
                let (key, value, expiry_opt) = (args[0].clone(), args[1].clone(), args[2].clone());

                if expiry_opt.to_lowercase().as_str() == "keepttl" {
                    return Ok(Self {
                        key,
                        value,
                        expiry: None,
                        keep_ttl: true,
                    });
                }
                if args.len() != 4 {
                    return Err(CommandError::InvalidInput);
                }
                let expiry_time = args[3].clone();
                let expiry = match expiry_opt.to_lowercase().as_str() {
                    "ex" => Duration::from_secs(expiry_time.parse::<u64>()?),
                    "px" => Duration::from_millis(expiry_time.parse::<u64>()?),
                    "exat" => Duration::from_secs(expiry_time.parse::<u64>()?),
                    "pxat" => Duration::from_millis(expiry_time.parse::<u64>()?),
                    _ => return Err(CommandError::InvalidCommand(expiry_opt.to_string())),
                };

                Ok(Self {
                    key,
                    value,
                    expiry: Some(expiry),
                    keep_ttl: false,
                })
            }
            _ => Err(CommandError::WrongNumArgs("set".to_string())),
        }
    }
}
