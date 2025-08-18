use std::env::args;

use codecrafters_redis::redis::{self};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let os_args: Vec<String> = args().collect();

    let mut port = "6379".to_string();
    let mut db_filename = String::new();
    let mut dir = String::new();
    let mut replica_of: Option<String> = None;

    if os_args.len() > 2 {
        os_args[1..]
            .iter()
            .step_by(2)
            .zip(os_args[2..].iter().step_by(2))
            .for_each(|(key, val)| match key.as_str() {
                "--dir" => dir = val.to_string(),
                "--port" => port = val.to_string(),
                "--replicaof" => replica_of = Some(val.to_string()),
                "--dbfilename" => db_filename = val.to_string(),
                _ => {}
            });
    }

    let config = codecrafters_redis::types::Config {
        dir,
        db_filename,
        port,
    };

    redis::init("127.0.0.1", config, replica_of).await
}
