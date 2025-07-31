use std::{net::TcpListener, sync::Arc, thread};

use crate::{
    commands::parse_array_command,
    db::RedisDatabase,
    resp::{self, Resp},
    RedisError,
};

pub(crate) type RedisResult = std::result::Result<Resp, RedisError>;

pub fn init(port: &str) -> Result<(), RedisError> {
    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).unwrap();

    let db = Arc::new(RedisDatabase::default());

    for stream in listener.incoming() {
        let clone_db = db.clone();
        match stream {
            Ok(mut stream) => {
                thread::spawn(move || handle_stream(&mut stream, clone_db.clone()).unwrap());
            }
            Err(err) => {
                println!("error: {err}");
            }
        }
    }

    Ok(())
}

pub fn handle_stream<S>(stream: &mut S, db: Arc<RedisDatabase>) -> Result<(), RedisError>
where
    S: std::io::Write + std::io::Read,
{
    let mut buf = [0; 512];
    loop {
        if let Ok(count) = stream.read(&mut buf) {
            if count == 0 {
                break;
            }
            parse_input(stream, &buf, db.clone())?
        }
    }
    Ok(())
}

fn parse_input<S>(stream: &mut S, buf: &[u8], db: Arc<RedisDatabase>) -> Result<(), RedisError>
where
    S: std::io::Write + std::io::Read,
{
    match resp::parse(buf) {
        Ok((input, _)) => match input {
            Resp::Array(contents) => match parse_array_command(&contents, db) {
                Ok(response) => response.write_to_writer(stream)?,
                Err(err) => Resp::SimpleError(format!("ERR {}", err)).write_to_writer(stream)?,
            },
            _ => Resp::SimpleError("ERR invalid command".to_string()).write_to_writer(stream)?,
        },

        Err(e) => Resp::SimpleError(format!("ERR {}", e)).write_to_writer(stream)?,
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, io::Read};

    use super::*;

    // #[test]
    // fn test_handle_stream() {
    //     let test_input = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
    //     let expected = b"$3\r\nhey\r\n";

    //     let mut writer = VecDeque::new();

    //     parse_input(&mut writer, test_input).unwrap();

    //     let mut results = [0u8; 9];
    //     writer.read_exact(&mut results).unwrap();
    //     assert_eq!(results, *expected);
    // }

    // #[test]
    // fn test_parse_command() {
    //     let test_input = vec![
    //         Resp::BulkString("echo".to_string()),
    //         Resp::BulkString("hey".to_string()),
    //     ];
    //     let expected = b"$3\r\nhey\r\n";
    //     let mut writer = VecDeque::new();

    //     parse_array_command(&test_input, &mut writer).unwrap();

    //     let mut results = [0u8; 9];
    //     writer.read_exact(&mut results).unwrap();
    //     assert_eq!(results, *expected);
    // }
}
