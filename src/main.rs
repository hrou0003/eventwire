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

            let response = Response {
                message_size: (4 + 2 + (2 + 2 + 2) + 2 + 4) as i32,
                header: Header {
                    request_api_key: 18,
                    request_api_version: header.request_api_version,
                    correlation_id: header.correlation_id,
                    client_id: None,
                },
                body: Body {
                    error_code: 0,
                    api_versions: vec![ApiVersion {
                        api_key: 18,
                        min_version: 1,
                        max_version: 4,
                    }],
                    tags: vec![],
                    throttle_time: 0,
                },
            };

            let response_bytes = response.to_be_bytes();

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

#[derive()]
pub struct Response {
    pub message_size: i32,
    pub header: Header,
    pub body: Body,
}

impl Response {
    pub fn to_be_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.message_size.to_be_bytes());
        buffer.extend_from_slice(&self.header.to_be_bytes());
        buffer.extend_from_slice(&self.body.to_be_bytes());
        buffer
    }
}

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

    pub fn to_be_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.correlation_id.to_be_bytes());
        buffer
    }
}

pub struct Body {
    pub error_code: i16,
    pub api_versions: Vec<ApiVersion>,
    pub throttle_time: i32,
    pub tags: Vec<u8>,
}

impl Body {
    pub fn to_be_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.error_code.to_be_bytes());
        let array_len = self.api_versions.len() as u32 + 1;
        buffer.extend_from_slice(&array_len.to_be_bytes());
        for version in &self.api_versions {
            buffer.extend_from_slice(&version.to_be_bytes());
        }
        buffer.extend_from_slice(&self.throttle_time.to_be_bytes());
        buffer.extend_from_slice(&0_i8.to_be_bytes());
        buffer
    }
}

pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

impl ApiVersion {
    pub fn to_be_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&self.api_key.to_be_bytes());
        buffer.extend_from_slice(&self.min_version.to_be_bytes());
        buffer.extend_from_slice(&self.max_version.to_be_bytes());
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
        let expected_bytes = vec![0, 0, 0, 0, 83, 249, 153, 23];
        let header = Header {
            request_api_key: 0,
            request_api_version: 0,
            correlation_id: 1408866583,
            client_id: None,
        };
        let actual_bytes = header.to_be_bytes();
        assert_eq!(actual_bytes, expected_bytes);
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

        let expected_hex_response = "0000000b001200046f7fc661ffff00";
        let expected_output = hex::decode(expected_hex_response).unwrap();

        assert_eq!(stream.output, expected_output);
    }
}
