use std::env::args;

use codecrafters_redis::redis::{self};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let os_args: Vec<String> = args().collect();
    let mut port_number = 6379;
    if os_args.len() > 1 && os_args[1] == "--port" {
        port_number = os_args[2].parse::<usize>()?;
    }

    // Uncomment this block to pass the first stage
    //
    redis::init(format!("127.0.0.1:{port_number}").as_str()).await
}
