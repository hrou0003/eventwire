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
    dbg!(message_size);

    let mut message_buffer = vec![0; message_size as usize];
    match stream.read(&mut message_buffer) {
        Err(e) => {
            dbg!("error: failed to read message body {}", e);
            return;
        }
        Ok(_) => {
            dbg!(&message_buffer);
        }
    }

    match Header::from_bytes(&message_buffer) {
        Ok(header) => {
            println!("Received header: {:?}", header);

            let response_header = Header {
                request_api_key: header.request_api_key,
                request_api_version: header.request_api_version,
                correlation_id: header.correlation_id,
                client_id: None,
            };

            println!("sending response header {:?}", header);

            let response_bytes = response_header.to_bytes();
            let response_len = response_bytes.len() as u32;

            dbg!(&response_bytes);

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

#[derive(Debug, PartialEq)]
pub struct Header {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl Header {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, std::io::Error> {
        let mut cursor = Cursor::new(bytes);
        let mut i16_buf = [0; 2];
        let mut i32_buf = [0; 4];

        cursor.read_exact(&mut i16_buf)?;
        let _ = i16::from_be_bytes(i16_buf);

        cursor.read_exact(&mut i16_buf)?;
        let _ = i16::from_be_bytes(i16_buf);

        cursor.read_exact(&mut i32_buf)?;
        let correlation_id = i32::from_be_bytes(i32_buf);

        Ok(Header {
            request_api_version: 0,
            request_api_key: 0,
            correlation_id,
            client_id: None,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.request_api_key.to_be_bytes());
        buffer.extend_from_slice(&self.request_api_version.to_be_bytes());
        buffer.extend_from_slice(&self.correlation_id.to_be_bytes());
        if let Some(client_id) = &self.client_id {
            buffer.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
        }
        buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Result;

    // A mock stream that reads from an input buffer and writes to an output buffer
    struct MockStream {
        input: Cursor<Vec<u8>>,
        output: Vec<u8>,
    }

    impl Read for MockStream {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
            self.input.read(buf)
        }
    }

    impl Write for MockStream {
        fn write(&mut self, buf: &[u8]) -> Result<usize> {
            self.output.write(buf)
        }

        fn flush(&mut self) -> Result<()> {
            self.output.flush()
        }
    }

    #[test]
    fn test_bytes_to_header() {
        let bytes: Vec<u8> = vec![0, 18, 0, 4, 83, 249, 153, 23];
        let expected_header = Header {
            request_api_key: 0,
            request_api_version: 0,
            correlation_id: 1408866583,
            client_id: None,
        };
        let header = Header::from_bytes(&bytes).unwrap();
        dbg!(&header);
        assert_eq!(header, expected_header);
    }

    #[test]
    fn test_header_to_bytes() {
        let hex_payload =
            "00000023001200046f7fc66100096b61666b612d636c69000a6b61666b612d636c6904302e3100";
        let input_bytes = hex::decode(hex_payload).expect("Failed to decode hex");
        let expected_header = Header {
            request_api_key: 0x0012,
            request_api_version: 0x0004,
            correlation_id: 0x6f7fc661,
            client_id: Some("kafka".to_string()),
        };
        let expected_bytes = expected_header.to_bytes();
        assert_eq!(input_bytes, expected_bytes);
    }

    #[test]
    fn test_handle_connection_with_provided_message() {
        // Hex string: 00000023001200046f7fc66100096b61666b612d636c6900
        let hex_payload =
            "00000023001200046f7fc66100096b61666b612d636c69000a6b61666b612d636c6904302e3100";
        let input_bytes = hex::decode(hex_payload).expect("Failed to decode hex");
        dbg!(&input_bytes);

        let mut stream = MockStream {
            input: Cursor::new(input_bytes),
            output: Vec::new(),
        };

        handle_connection(&mut stream);

        // Based on the logic in to_bytes(), the response should contain:
        // - api_key (i16): 0x0012
        // - api_version (i16): 0x0004
        // - correlation_id (i32): 0x6f7fc661
        // - client_id_len (i16): -1 (0xffff) for None
        // - tagged_fields (u8): 0 for zero tagged fields
        // Total payload length is 2+2+4+2+1 = 11 bytes (0x0b)
        // The full response is the size prefix + payload.
        let expected_hex_response = "0000000b001200046f7fc661ffff00";
        let expected_output = hex::decode(expected_hex_response).unwrap();

        assert_eq!(stream.output, expected_output);
    }
}
