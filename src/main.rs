use std::env::args;

use codecrafters_redis::redis::{self};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let os_args: Vec<String> = args().collect();
    let mut port_number = 6379;
    if os_args.len() > 1 && os_args[1] == "--port" {
        port_number = os_args[2].parse::<usize>()?;
    }

    let mut replica_of = None;

    if os_args.len() > 3 && os_args[3] == "--replicaof" {
        replica_of = Some(os_args[4].to_string());
    }

    // Uncomment this block to pass the first stage
    //
    redis::init("127.0.0.1", &port_number.to_string(), replica_of).await
}
