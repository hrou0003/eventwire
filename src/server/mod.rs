use crate::codec::KafkaCodec;
use crate::protocol::Request;
use crate::state::ApiRegistry;
use std::io::{self, Read, Write};
use std::net::TcpListener;

const LISTEN_ADDR: &str = "127.0.0.1:9092";

pub fn run() -> io::Result<()> {
    println!("starting tcp listener on {LISTEN_ADDR}");
    let registry = ApiRegistry::default();

    let listener = TcpListener::bind(LISTEN_ADDR)?;
    println!("listener bound on {LISTEN_ADDR}");

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let peer = stream
                    .peer_addr()
                    .map(|addr| addr.to_string())
                    .unwrap_or_else(|_| "<unknown>".into());
                println!("accepted connection from {peer}");

                if let Err(err) = handle_connection(&mut stream, &registry) {
                    eprintln!("connection error from {peer}: {err}");
                }
            }
            Err(err) => eprintln!("listener error: {err}"),
        }
    }

    Ok(())
}

fn handle_connection(stream: &mut impl ReadWrite, registry: &ApiRegistry) -> io::Result<()> {
    let request = KafkaCodec::read_request(stream)?;
    let response = match request {
        Request::ApiVersions(request) => {
            println!(
                "processing ApiVersions request key={} version={} correlation={}",
                request.api_key, request.api_version, request.correlation_id
            );
            registry.handle_versions(request)
        }
    };

    KafkaCodec::write_response(stream, &response)?;
    println!("response sent");
    Ok(())
}

trait ReadWrite: Read + Write {}
impl<T: Read + Write> ReadWrite for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::MessageFramer;
    use std::convert::{TryFrom, TryInto};
    use std::io::{self, Cursor, Read, Write};

    struct MockStream {
        input: Cursor<Vec<u8>>,
        output: Vec<u8>,
    }

    impl MockStream {
        fn new(bytes: Vec<u8>) -> Self {
            Self {
                input: Cursor::new(bytes),
                output: Vec::new(),
            }
        }
    }

    impl Read for MockStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.input.read(buf)
        }
    }

    impl Write for MockStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.output.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn handle_connection_writes_successful_response() {
        let request = build_request(18, 4, 7, Some("client"));
        let mut stream = MockStream::new(request);
        let registry = ApiRegistry::default();

        handle_connection(&mut stream, &registry).expect("handle_connection should succeed");

        let response = stream.output;
        assert!(response.len() > 4);

        let declared_length = u32::from_be_bytes(response[0..4].try_into().unwrap()) as usize;
        assert_eq!(declared_length, response.len() - 4);

        let correlation_id = i32::from_be_bytes(response[4..8].try_into().unwrap());
        assert_eq!(correlation_id, 7);

        let error_code = i16::from_be_bytes(response[9..11].try_into().unwrap());
        assert_eq!(error_code, 0);

        let version_count = u16::from_be_bytes(response[11..13].try_into().unwrap());
        assert!(version_count > 0);
    }

    #[test]
    fn handle_connection_rejects_unknown_api_key() {
        let request = build_request(7, 0, 13, None);
        let mut stream = MockStream::new(request);
        let registry = ApiRegistry::default();

        handle_connection(&mut stream, &registry).expect("handle_connection should succeed");

        let response = stream.output;

        let error_code = i16::from_be_bytes(response[9..11].try_into().unwrap());
        assert_eq!(error_code, 35);

        let version_count = u16::from_be_bytes(response[11..13].try_into().unwrap());
        assert_eq!(version_count, 0);
    }

    fn build_request(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<&str>,
    ) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&api_key.to_be_bytes());
        payload.extend_from_slice(&api_version.to_be_bytes());
        payload.extend_from_slice(&correlation_id.to_be_bytes());

        match client_id {
            Some(value) => {
                let length = i16::try_from(value.len()).expect("client id is too long");
                payload.extend_from_slice(&length.to_be_bytes());
                payload.extend_from_slice(value.as_bytes());
            }
            None => payload.extend_from_slice(&(-1_i16).to_be_bytes()),
        }

        payload.push(0);

        MessageFramer::frame(&payload).expect("frame should succeed")
    }
}
