use std::env::args;

use codecrafters_redis::redis::{self};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let os_args: Vec<String> = args().collect();
    println!("os_args: {os_args:#?}");

    let mut port = "6379".to_string();
    let mut db_filename = String::new();
    let mut dir = String::new();
    let mut replica_of: Option<String> = None;

    os_args[1..]
        .iter()
        .step_by(2)
        .zip(os_args[2..].iter().step_by(2))
        .for_each(|(key, val)| match key.as_str() {
            "--dir" => dir = val.to_string(),
            "--port" => port = val.to_string(),
            "--replica_of" => replica_of = Some(val.to_string()),
            "--dbfilename" => db_filename = val.to_string(),
            _ => {}
        });
    let config = codecrafters_redis::types::Config {
        dir,
        db_filename,
        port,
    };

    println!("config: {config:#?}");
    redis::init("127.0.0.1", config, replica_of).await
}
