use std::io::{self, Read};

#[cfg(test)]
use std::io::Write;

/// Utilities for reading and writing Kafka length-prefixed messages.
pub struct MessageFramer;

impl MessageFramer {
    const LENGTH_PREFIX_SIZE: usize = 4;

    /// Reads a single length-prefixed payload from `stream`.
    pub fn read(stream: &mut impl Read) -> io::Result<Vec<u8>> {
        let mut len_buf = [0_u8; Self::LENGTH_PREFIX_SIZE];
        stream.read_exact(&mut len_buf)?;
        let length = u32::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0_u8; length];
        stream.read_exact(&mut payload)?;
        Ok(payload)
    }
}

#[cfg(test)]
impl MessageFramer {
    /// Writes a length-prefixed payload to `stream`.
    pub fn write(stream: &mut impl Write, payload: &[u8]) -> io::Result<()> {
        let length = u32::try_from(payload.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "payload too large"))?;
        stream.write_all(&length.to_be_bytes())?;
        stream.write_all(payload)?;
        Ok(())
    }

    /// Returns the framed bytes for the given `payload`.
    pub fn frame(payload: &[u8]) -> io::Result<Vec<u8>> {
        let length = u32::try_from(payload.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "payload too large"))?;
        let mut buffer = Vec::with_capacity(Self::LENGTH_PREFIX_SIZE + payload.len());
        buffer.extend_from_slice(&length.to_be_bytes());
        buffer.extend_from_slice(payload);
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::MessageFramer;
    use std::io::Cursor;

    #[test]
    fn read_returns_original_payload() {
        let payload = vec![1, 2, 3, 4];
        let framed = MessageFramer::frame(&payload).expect("frame should succeed");
        let mut cursor = Cursor::new(framed);

        let decoded = MessageFramer::read(&mut cursor).expect("read should succeed");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn write_produces_expected_framed_bytes() {
        let payload = vec![9, 8, 7];
        let mut cursor = Cursor::new(Vec::new());

        MessageFramer::write(&mut cursor, &payload).expect("write should succeed");
        let captured = cursor.into_inner();
        let expected = MessageFramer::frame(&payload).expect("frame should succeed");
        assert_eq!(captured, expected);
    }
}
