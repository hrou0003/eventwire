pub mod framing;
pub mod primitives;
pub mod request_decoder;

pub use framing::MessageFramer;
pub use request_decoder::RequestDecoder;

use crate::protocol::{ApiVersionsRequest, ApiVersionsResponse, Request, RequestHeader};
use std::io::{self, Cursor, Read, Write};

pub struct KafkaCodec;

impl KafkaCodec {
    pub fn read_request(stream: &mut impl Read) -> io::Result<Request> {
        let payload = MessageFramer::read(stream)?;
        let mut cursor = Cursor::new(payload.as_slice());
        let header = RequestDecoder::read_header(&mut cursor)?;
        Ok(Request::ApiVersions(Self::build_api_versions_request(
            header,
        )))
    }

    pub fn write_response(
        stream: &mut impl Write,
        response: &ApiVersionsResponse,
    ) -> io::Result<()> {
        stream.write_all(&response.to_bytes())
    }

    fn build_api_versions_request(header: RequestHeader) -> ApiVersionsRequest {
        ApiVersionsRequest {
            api_key: header.request_api_key,
            api_version: header.request_api_version,
            correlation_id: header.correlation_id,
            client_id: header.client_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{KafkaCodec, MessageFramer};
    use crate::protocol::{
        api_versions::ApiVersionsResponseBody, ApiVersion, ApiVersionsResponse, Request,
    };
    use std::convert::{TryFrom, TryInto};
    use std::io::{self, Cursor, Read, Write};

    struct MockStream {
        input: Cursor<Vec<u8>>,
        output: Vec<u8>,
    }

    impl MockStream {
        fn with_bytes(bytes: Vec<u8>) -> Self {
            Self {
                input: Cursor::new(bytes),
                output: Vec::new(),
            }
        }

        fn empty() -> Self {
            Self::with_bytes(Vec::new())
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
    fn decodes_api_versions_request() {
        let payload = build_request(18, 4, 7, Some("client"));
        let framed = MessageFramer::frame(&payload).expect("frame should succeed");
        let mut stream = MockStream::with_bytes(framed);

        let decoded = KafkaCodec::read_request(&mut stream).expect("request should decode");

        match decoded {
            Request::ApiVersions(actual) => {
                assert_eq!(actual.api_key, 18);
                assert_eq!(actual.api_version, 4);
                assert_eq!(actual.correlation_id, 7);
                assert_eq!(actual.client_id.as_deref(), Some("client"));
            }
        }
    }

    #[test]
    fn writes_response_bytes() {
        let body = ApiVersionsResponseBody::new(
            0,
            vec![
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
            ],
            0,
        );
        let response = ApiVersionsResponse::new(99, body);
        let mut stream = MockStream::empty();

        KafkaCodec::write_response(&mut stream, &response).expect("response should serialize");

        assert!(stream.output.len() > 4);
        let declared_len = u32::from_be_bytes(stream.output[0..4].try_into().unwrap()) as usize;
        assert_eq!(declared_len, stream.output.len() - 4);

        let correlation_id = i32::from_be_bytes(stream.output[4..8].try_into().unwrap());
        assert_eq!(correlation_id, 99);
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

        payload
    }
}
