use std::io::{self, Cursor};

/// Reads a big-endian `i16` from the provided cursor.
pub fn read_i16(cursor: &mut Cursor<&[u8]>) -> io::Result<i16> {
    use std::io::Read as _;
    let mut buf = [0_u8; 2];
    cursor.read_exact(&mut buf)?;
    Ok(i16::from_be_bytes(buf))
}

/// Reads a big-endian `i32` from the provided cursor.
pub fn read_i32(cursor: &mut Cursor<&[u8]>) -> io::Result<i32> {
    use std::io::Read as _;
    let mut buf = [0_u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

/// Reads an optional UTF-8 string according to the Kafka protocol rules.
///
/// A length of `-1` represents `None`. Any other non-negative length indicates the
/// number of bytes to read. Invalid UTF-8 data returns `io::ErrorKind::InvalidData`.
pub fn read_nullable_string(cursor: &mut Cursor<&[u8]>) -> io::Result<Option<String>> {
    use std::io::Read as _;
    let length = read_i16(cursor)?;
    if length < 0 {
        return Ok(None);
    }

    let length = length as usize;
    let mut buffer = vec![0_u8; length];
    cursor.read_exact(&mut buffer)?;
    String::from_utf8(buffer)
        .map(Some)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn read_i16_returns_big_endian_value() {
        let data = [0x12, 0x34];
        let mut cursor = Cursor::new(&data[..]);
        let value = read_i16(&mut cursor).expect("read should succeed");
        assert_eq!(value, 0x1234);
    }

    #[test]
    fn read_i32_returns_big_endian_value() {
        let data = [0x12, 0x34, 0x56, 0x78];
        let mut cursor = Cursor::new(&data[..]);
        let value = read_i32(&mut cursor).expect("read should succeed");
        assert_eq!(value, 0x12345678);
    }

    #[test]
    fn read_nullable_string_handles_none() {
        let data = [0xFF, 0xFF];
        let mut cursor = Cursor::new(&data[..]);
        let value = read_nullable_string(&mut cursor).expect("read should succeed");
        assert!(value.is_none());
    }

    #[test]
    fn read_nullable_string_reads_utf8_string() {
        let mut data = Vec::from([0x00, 0x05]);
        data.extend_from_slice(b"kafka");
        let mut cursor = Cursor::new(data.as_slice());

        let value = read_nullable_string(&mut cursor).expect("read should succeed");
        assert_eq!(value.as_deref(), Some("kafka"));
    }

    #[test]
    fn read_nullable_string_handles_invalid_utf8() {
        let mut data = Vec::from([0x00, 0x02]);
        data.extend_from_slice(&[0xFF, 0xFF]);
        let mut cursor = Cursor::new(data.as_slice());

        let err = read_nullable_string(&mut cursor).expect_err("read should fail");
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn read_nullable_string_errors_on_truncated_input() {
        let data = [0x00, 0x04, 0x01];
        let mut cursor = Cursor::new(&data[..]);
        let err = read_nullable_string(&mut cursor).expect_err("read should fail");
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn read_functions_propagate_short_reads() {
        let data = [0x12];
        let mut cursor = Cursor::new(&data[..]);
        let err = read_i16(&mut cursor).expect_err("read should fail");
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);

        let data = [0x12, 0x34, 0x56];
        let mut cursor = Cursor::new(&data[..]);
        let err = read_i32(&mut cursor).expect_err("read should fail");
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }
}
