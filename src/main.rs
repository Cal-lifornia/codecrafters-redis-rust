use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                thread::spawn(move || {
                    let mut buf = [0; 512];
                    loop {
                        if let Ok(count) = stream.read(&mut buf) {
                            if count == 0 {
                                break;
                            } else {
                                stream.write(b"+PONG\r\n").unwrap();
                            }
                        }
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
