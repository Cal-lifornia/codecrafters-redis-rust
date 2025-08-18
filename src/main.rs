use std::env::args;

use codecrafters_redis::redis::{self};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let os_args: Vec<String> = args().collect();

    let mut port = "6379".to_string();
    let mut db_filename = String::new();
    let mut dir = String::new();
    let mut replica_of: Option<String> = None;

    os_args[1..]
        .iter()
        .skip(2)
        .zip(os_args[2..].iter().skip(2))
        .for_each(|(key, val)| match key.as_str() {
            "--port" => port = val.to_string(),
            "--replica_of" => replica_of = Some(val.to_string()),
            "--dir" => dir = val.to_string(),
            "--dbfilename" => db_filename = val.to_string(),
            _ => {}
        });

    // Uncomment this block to pass the first stage
    //
    redis::init(
        "127.0.0.1",
        codecrafters_redis::types::Config {
            dir,
            db_filename,
            port,
        },
        replica_of,
    )
    .await
}
