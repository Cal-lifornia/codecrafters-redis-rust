use std::{net::TcpListener, sync::OnceLock, thread};

use anyhow::Result;

use crate::{
    commands::parse_array_command,
    db::DB,
    resp::{self, Resp},
};

pub static DB_CELL: OnceLock<DB> = OnceLock::new();

pub fn init(port: &str) -> Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    DB_CELL.get_or_init(|| DB::new());

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                thread::spawn(move || handle_stream(&mut stream)?);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}

pub fn handle_stream<S>(stream: &mut S) -> Result<()>
where
    S: std::io::Write + std::io::Read,
{
    let mut buf = [0; 512];
    loop {
        if let Ok(count) = stream.read(&mut buf) {
            if count == 0 {
                break;
            }
            parse_input(stream, &buf)?
        }
    }
    Ok(())
}

fn parse_input<S>(stream: &mut S, buf: &[u8]) -> Result<()>
where
    S: std::io::Write + std::io::Read,
{
    match resp::parse(buf) {
        Ok((input, _)) => match input {
            Resp::Array(contents) => match parse_array_command(&contents, stream) {
                Ok(_) => {}
                Err(err) => Resp::SimpleError(format!("ERR {}", err)).write_to_writer(stream)?,
            },
            // Resp::SimpleString(s) => match parse_simple_command(s, stream) {
            //     Ok(_) => {}
            //     Err(err) => Resp::SimpleError(format!("ERR {}", err)).write_to_writer(stream)?,
            // },
            _ => Resp::SimpleError("ERR invalid command".to_string()).write_to_writer(stream)?,
        },

        Err(e) => Resp::SimpleError(format!("ERR {}", e)).write_to_writer(stream)?,
    };
    Ok(())
}

// fn parse_simple_command<S>(input: String, stream: &mut S) -> Result<(), RedisError>
// where
//     S: std::io::Write + std::io::Read,
// {
//     #[allow(clippy::single_match)]
//     match input.to_lowercase().as_str() {
//         "ping" => match Resp::SimpleString("PONG".to_string()).write_to_writer(stream) {
//             Ok(_) => {}
//             Err(err) => return Err(RedisError::Other(err.into())),
//         },
//         _ => {}
//     };
//     Ok(())
// }

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, io::Read};

    use super::*;

    #[test]
    fn test_handle_stream() {
        let test_input = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
        let expected = b"$3\r\nhey\r\n";

        let mut writer = VecDeque::new();

        parse_input(&mut writer, test_input).unwrap();

        let mut results = [0u8; 9];
        writer.read_exact(&mut results).unwrap();
        assert_eq!(results, *expected);
    }

    #[test]
    fn test_parse_command() {
        let test_input = vec![
            Resp::BulkString("echo".to_string()),
            Resp::BulkString("hey".to_string()),
        ];
        let expected = b"$3\r\nhey\r\n";
        let mut writer = VecDeque::new();

        parse_array_command(&test_input, &mut writer).unwrap();

        let mut results = [0u8; 9];
        writer.read_exact(&mut results).unwrap();
        assert_eq!(results, *expected);
    }

    // #[test]
    // fn test_parse_simple_command() {
    //     let test_input: Vec<String> =
    //         vec!["PING".to_string(), "ping".to_string(), "pInG".to_string()];
    //     let expected = b"+PONG\r\n";

    //     let mut writer = VecDeque::new();
    //     for input in test_input {
    //         parse_simple_command(input, &mut writer).unwrap();

    //         let mut results = [0u8; 7];
    //         writer.read_exact(&mut results).unwrap();
    //         assert_eq!(results, *expected);
    //         writer.clear()
    //     }
    // }
}
