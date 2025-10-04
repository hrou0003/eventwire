use std::convert::TryFrom;
use std::io::{self, Cursor, Read, Write};
use std::net::TcpListener;

const LISTEN_ADDR: &str = "127.0.0.1:9092";
const API_VERSIONS_KEY: i16 = 18;
const SUPPORTED_MIN_VERSION: i16 = 0;
const SUPPORTED_MAX_VERSION: i16 = 4;
const DEFAULT_THROTTLE_TIME_MS: i32 = 0;

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind(LISTEN_ADDR).expect("failed to bind TCP listener");

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                if let Err(err) = handle_connection(&mut stream) {
                    eprintln!("connection error: {err}");
                }
            }
            Err(err) => eprintln!("listener error: {err}"),
        }
    }
}

fn handle_connection(stream: &mut impl ReadWrite) -> io::Result<()> {
    let message = read_message(stream)?;
    let header = RequestHeader::from_bytes(&message)?;

    let error_code = if header.request_api_key == API_VERSIONS_KEY
        && (SUPPORTED_MIN_VERSION..=SUPPORTED_MAX_VERSION).contains(&header.request_api_version)
    {
        0
    } else {
        35
    };

    let api_versions = if error_code == 0 {
        SUPPORTED_API_VERSIONS.to_vec()
    } else {
        Vec::new()
    };

    let response = ApiVersionsResponse::new(
        header.correlation_id,
        ApiVersionsResponseBody::new(error_code, api_versions, DEFAULT_THROTTLE_TIME_MS),
    );

    stream.write_all(&response.to_bytes())?;
    Ok(())
}

fn read_message(stream: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut size_buffer = [0_u8; 4];
    stream.read_exact(&mut size_buffer)?;
    let message_size = u32::from_be_bytes(size_buffer) as usize;

    let mut message = vec![0_u8; message_size];
    stream.read_exact(&mut message)?;

    Ok(message)
}

trait ReadWrite: Read + Write {}
impl<T: Read + Write> ReadWrite for T {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RequestHeader {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: Option<String>,
}

impl RequestHeader {
    fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let request_api_key = read_i16(&mut cursor)?;
        let request_api_version = read_i16(&mut cursor)?;
        let correlation_id = read_i32(&mut cursor)?;
        let client_id = read_nullable_string(&mut cursor)?;

        Ok(Self {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
        })
    }
}

fn read_i16(cursor: &mut Cursor<&[u8]>) -> io::Result<i16> {
    let mut buf = [0_u8; 2];
    cursor.read_exact(&mut buf)?;
    Ok(i16::from_be_bytes(buf))
}

fn read_i32(cursor: &mut Cursor<&[u8]>) -> io::Result<i32> {
    let mut buf = [0_u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

fn read_nullable_string(cursor: &mut Cursor<&[u8]>) -> io::Result<Option<String>> {
    let mut len_buf = [0_u8; 2];
    match cursor.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }

    let length = i16::from_be_bytes(len_buf);
    if length < 0 {
        return Ok(None);
    }

    let mut buffer = vec![0_u8; length as usize];
    cursor.read_exact(&mut buffer)?;
    String::from_utf8(buffer)
        .map(Some)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ApiVersion {
    api_key: i16,
    min_version: i16,
    max_version: i16,
}

impl ApiVersion {
    fn to_bytes(self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(7);
        buffer.extend_from_slice(&self.api_key.to_be_bytes());
        buffer.extend_from_slice(&self.min_version.to_be_bytes());
        buffer.extend_from_slice(&self.max_version.to_be_bytes());
        buffer.push(0);
        buffer
    }
}

const SUPPORTED_API_VERSIONS: [ApiVersion; 3] = [
    ApiVersion {
        api_key: 17,
        min_version: 0,
        max_version: 4,
    },
    ApiVersion {
        api_key: 18,
        min_version: 0,
        max_version: 4,
    },
    ApiVersion {
        api_key: 19,
        min_version: 0,
        max_version: 4,
    },
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResponseHeader {
    correlation_id: i32,
}

impl ResponseHeader {
    fn to_bytes(self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(5);
        buffer.extend_from_slice(&self.correlation_id.to_be_bytes());
        buffer.push(0);
        buffer
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ApiVersionsResponseBody {
    error_code: i16,
    api_versions: Vec<ApiVersion>,
    throttle_time_ms: i32,
}

impl ApiVersionsResponseBody {
    fn new(error_code: i16, api_versions: Vec<ApiVersion>, throttle_time_ms: i32) -> Self {
        Self {
            error_code,
            api_versions,
            throttle_time_ms,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        buffer.extend_from_slice(&self.error_code.to_be_bytes());

        let version_count =
            u16::try_from(self.api_versions.len()).expect("api_versions length exceeds u16::MAX");
        buffer.extend_from_slice(&version_count.to_be_bytes());

        for version in &self.api_versions {
            buffer.extend_from_slice(&version.to_bytes());
        }

        buffer.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
        buffer.push(0);

        buffer
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ApiVersionsResponse {
    header: ResponseHeader,
    body: ApiVersionsResponseBody,
}

impl ApiVersionsResponse {
    fn new(correlation_id: i32, body: ApiVersionsResponseBody) -> Self {
        Self {
            header: ResponseHeader { correlation_id },
            body,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&self.header.to_bytes());
        payload.extend_from_slice(&self.body.to_bytes());

        let mut buffer = Vec::with_capacity(4 + payload.len());
        buffer.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buffer.extend_from_slice(&payload);
        buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::{TryFrom, TryInto};
    use std::io::{self, Cursor, Read, Write};

    struct MockStream {
        input: Cursor<Vec<u8>>,
        output: Vec<u8>,
    }

    impl MockStream {
        fn new(input: Vec<u8>) -> Self {
            Self {
                input: Cursor::new(input),
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

        let mut message = Vec::with_capacity(4 + payload.len());
        message.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        message.extend_from_slice(&payload);
        message
    }

    #[test]
    fn parses_request_header_with_client_id() {
        let request = build_request(API_VERSIONS_KEY, 4, 42, Some("kafka-cli"));
        let header = RequestHeader::from_bytes(&request[4..]).expect("header should parse");

        assert_eq!(header.request_api_key, API_VERSIONS_KEY);
        assert_eq!(header.request_api_version, 4);
        assert_eq!(header.correlation_id, 42);
        assert_eq!(header.client_id.as_deref(), Some("kafka-cli"));
    }

    #[test]
    fn response_serialization_includes_length_prefix() {
        let body = ApiVersionsResponseBody::new(
            0,
            SUPPORTED_API_VERSIONS.to_vec(),
            DEFAULT_THROTTLE_TIME_MS,
        );
        let response = ApiVersionsResponse::new(99, body);
        let bytes = response.to_bytes();

        let length = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
        assert_eq!(length, bytes.len() - 4);

        let correlation_id = i32::from_be_bytes(bytes[4..8].try_into().unwrap());
        assert_eq!(correlation_id, 99);
    }

    #[test]
    fn handle_connection_writes_supported_response() {
        let request = build_request(API_VERSIONS_KEY, SUPPORTED_MAX_VERSION, 7, Some("client"));
        let mut stream = MockStream::new(request);

        handle_connection(&mut stream).expect("handle_connection should succeed");

        let response = stream.output;
        assert!(response.len() > 4);

        let declared_length = u32::from_be_bytes(response[0..4].try_into().unwrap()) as usize;
        assert_eq!(declared_length, response.len() - 4);

        let error_code = i16::from_be_bytes(response[9..11].try_into().unwrap());
        assert_eq!(error_code, 0);

        let version_count = u16::from_be_bytes(response[11..13].try_into().unwrap());
        assert_eq!(usize::from(version_count), SUPPORTED_API_VERSIONS.len());

        let throttle_offset = 13 + version_count as usize * 7;
        let throttle_time = i32::from_be_bytes(
            response[throttle_offset..throttle_offset + 4]
                .try_into()
                .unwrap(),
        );
        assert_eq!(throttle_time, DEFAULT_THROTTLE_TIME_MS);

        let final_tag_index = throttle_offset + 4;
        assert_eq!(response[final_tag_index], 0);
    }

    #[test]
    fn handle_connection_rejects_unknown_api_key() {
        let request = build_request(7, SUPPORTED_MIN_VERSION, 13, None);
        let mut stream = MockStream::new(request);

        handle_connection(&mut stream).expect("handle_connection should succeed");

        let response = stream.output;
        let error_code = i16::from_be_bytes(response[9..11].try_into().unwrap());
        assert_eq!(error_code, 35);

        let version_count = u16::from_be_bytes(response[11..13].try_into().unwrap());
        assert_eq!(version_count, 0);
    }
}
