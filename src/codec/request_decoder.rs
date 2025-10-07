use super::primitives;
use crate::protocol::RequestHeader;
use std::io::{self, Cursor};

pub struct RequestDecoder;

impl RequestDecoder {
    pub fn read_header(cursor: &mut Cursor<&[u8]>) -> io::Result<RequestHeader> {
        let request_api_key = primitives::read_i16(cursor)?;
        let request_api_version = primitives::read_i16(cursor)?;
        let correlation_id = primitives::read_i32(cursor)?;
        let client_id = primitives::read_nullable_string(cursor)?;

        Ok(RequestHeader {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;
    use std::io::{Cursor, ErrorKind};

    #[test]
    fn decodes_header_with_client_id() {
        let bytes = build_header_bytes(18, 4, 7, Some("client"));
        let mut cursor = Cursor::new(bytes.as_slice());

        let header = RequestDecoder::read_header(&mut cursor).expect("header should decode");

        assert_eq!(header.request_api_key, 18);
        assert_eq!(header.request_api_version, 4);
        assert_eq!(header.correlation_id, 7);
        assert_eq!(header.client_id.as_deref(), Some("client"));
    }

    #[test]
    fn decodes_header_without_client_id() {
        let bytes = build_header_bytes(18, 4, 7, None);
        let mut cursor = Cursor::new(bytes.as_slice());

        let header = RequestDecoder::read_header(&mut cursor).expect("header should decode");

        assert_eq!(header.request_api_key, 18);
        assert_eq!(header.request_api_version, 4);
        assert_eq!(header.correlation_id, 7);
        assert_eq!(header.client_id, None);
    }

    #[test]
    fn errors_on_truncated_payload() {
        let mut bytes = build_header_bytes(18, 4, 7, Some("client"));
        bytes.truncate(5);
        let mut cursor = Cursor::new(bytes.as_slice());

        let err =
            RequestDecoder::read_header(&mut cursor).expect_err("header decoding should fail");
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }

    fn build_header_bytes(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<&str>,
    ) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&api_key.to_be_bytes());
        bytes.extend_from_slice(&api_version.to_be_bytes());
        bytes.extend_from_slice(&correlation_id.to_be_bytes());

        match client_id {
            Some(value) => {
                let len = i16::try_from(value.len()).expect("client id too long");
                bytes.extend_from_slice(&len.to_be_bytes());
                bytes.extend_from_slice(value.as_bytes());
            }
            None => bytes.extend_from_slice(&(-1_i16).to_be_bytes()),
        }

        bytes
    }
}
