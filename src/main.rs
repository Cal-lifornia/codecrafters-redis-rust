#[tokio::main]
async fn main() -> std::io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let mut args = std::env::args();
    let mut port = None::<String>;
    while let Some(arg) = args.next() {
        if arg.to_lowercase().as_str() == "--port" {
            port = args.next();
        }
    }
    codecrafters_redis::run(port).await
}
