use codecrafters_redis::handle_stream;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                let n = match socket.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(n) => n,
                    Err(err) => {
                        eprintln!("failed to read from socket; err = {:?}", err);
                        return;
                    }
                };
                let results = handle_stream(&buf[0..n]).await.expect("valid results");
                socket.write_all(&results).await.expect("valid write");
            }
        });
    }
}
