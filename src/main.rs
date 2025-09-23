use std::io::{Cursor, Read, Write};
use std::net::TcpListener;

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                handle_connection(&mut stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(stream: &mut impl ReadWrite) {
    let mut size_buffer = [0; 4];
    if stream.read_exact(&mut size_buffer).is_err() {
        println!("error: failed to read message size");
        return;
    }
    let message_size = u32::from_be_bytes(size_buffer);

    let mut message_buffer = vec![0; message_size as usize];
    if stream.read_exact(&mut message_buffer).is_err() {
        println!("error: failed to read message body");
        return;
    }

    match Header::from_bytes(&message_buffer) {
        Ok(header) => {
            println!("Received header: {:?}", header);

            let response_header = Header {
                request_api_key: header.request_api_key,
                request_api_version: header.request_api_version,
                correlation_id: header.correlation_id,
                client_id: None,
                tag_buffer: None,
            };

            println!("sending response header {:?}", header);

            let response_bytes = response_header.to_bytes();
            let response_len = response_bytes.len() as u32;

            if stream.write_all(&response_len.to_be_bytes()).is_err() {
                println!("Error writing response size");
                return;
            }

            if stream.write_all(&response_bytes).is_err() {
                println!("Error writing response message");
                return;
            }
            println!("Wrote response message");
        }
        Err(e) => {
            println!("error: failed to deserialize header: {}", e);
        }
    }
}

// A trait for mocking the stream in tests
trait ReadWrite: Read + Write {}
impl<T: Read + Write> ReadWrite for T {}

#[derive(Debug)]
pub struct Header {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub tag_buffer: Option<Vec<u8>>,
}

impl Header {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, std::io::Error> {
        let mut cursor = Cursor::new(bytes);
        let mut i16_buf = [0; 2];
        let mut i32_buf = [0; 4];

        cursor.read_exact(&mut i16_buf)?;
        let request_api_key = i16::from_be_bytes(i16_buf);

        cursor.read_exact(&mut i16_buf)?;
        let request_api_version = i16::from_be_bytes(i16_buf);

        cursor.read_exact(&mut i32_buf)?;
        let correlation_id = i32::from_be_bytes(i32_buf);

        cursor.read_exact(&mut i16_buf)?;
        let client_id_len = i16::from_be_bytes(i16_buf);
        let client_id = if client_id_len == -1 {
            None
        } else {
            let mut str_buf = vec![0; client_id_len as usize];
            cursor.read_exact(&mut str_buf)?;
            Some(String::from_utf8(str_buf).unwrap())
        };

        let remaining_len = bytes.len() - cursor.position() as usize;
        let tag_buffer = if remaining_len > 0 {
            let mut buf = vec![0; remaining_len];
            cursor.read_exact(&mut buf)?;
            Some(buf)
        } else {
            None
        };

        Ok(Header {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
            tag_buffer,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.request_api_key.to_be_bytes());
        buffer.extend_from_slice(&self.request_api_version.to_be_bytes());
        buffer.extend_from_slice(&self.correlation_id.to_be_bytes());
        buffer.extend_from_slice(&(-1i16).to_be_bytes());
        buffer.push(0u8); // 0 tagged fields
        buffer
    }
}
