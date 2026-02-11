//! Database file header (stored in the first 100 bytes of page 0).

use std::io::{self, Read, Write};

/// Magic bytes identifying a ralph-sqlite database file.
pub const MAGIC: &[u8; 16] = b"ralph-sqlite\0\0\0\0";

/// Default page size in bytes.
pub const DEFAULT_PAGE_SIZE: u32 = 4096;

/// Size of the file header in bytes.
pub const HEADER_SIZE: usize = 100;

/// The file header stored at the beginning of the database file (page 0).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    /// Page size in bytes (must be a power of 2, >= 512).
    pub page_size: u32,
    /// Total number of pages in the database file.
    pub page_count: u32,
    /// Page number of the first freelist trunk page (0 = no freelist).
    pub freelist_head: u32,
    /// Total number of pages on the freelist.
    pub freelist_count: u32,
    /// Page number of the schema table root page.
    pub schema_root: u32,
}

impl Default for FileHeader {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            page_count: 1, // just the header page itself
            freelist_head: 0,
            freelist_count: 0,
            schema_root: 0,
        }
    }
}

impl FileHeader {
    /// Serialize the header to a 100-byte buffer.
    pub fn serialize(&self, buf: &mut [u8]) {
        assert!(buf.len() >= HEADER_SIZE, "buffer too small for header");
        // Zero out the header area
        buf[..HEADER_SIZE].fill(0);

        // Bytes 0..16: magic
        buf[0..16].copy_from_slice(MAGIC);
        // Bytes 16..20: page_size (big-endian)
        buf[16..20].copy_from_slice(&self.page_size.to_be_bytes());
        // Bytes 20..24: page_count
        buf[24..28].copy_from_slice(&self.page_count.to_be_bytes());
        // Bytes 28..32: freelist_head
        buf[28..32].copy_from_slice(&self.freelist_head.to_be_bytes());
        // Bytes 32..36: freelist_count
        buf[32..36].copy_from_slice(&self.freelist_count.to_be_bytes());
        // Bytes 36..40: schema_root
        buf[36..40].copy_from_slice(&self.schema_root.to_be_bytes());
    }

    /// Deserialize a header from a buffer. Returns `None` if magic doesn't match.
    pub fn deserialize(buf: &[u8]) -> Option<Self> {
        if buf.len() < HEADER_SIZE {
            return None;
        }
        if &buf[0..16] != MAGIC.as_slice() {
            return None;
        }
        let page_size = u32::from_be_bytes(buf[16..20].try_into().ok()?);
        let page_count = u32::from_be_bytes(buf[24..28].try_into().ok()?);
        let freelist_head = u32::from_be_bytes(buf[28..32].try_into().ok()?);
        let freelist_count = u32::from_be_bytes(buf[32..36].try_into().ok()?);
        let schema_root = u32::from_be_bytes(buf[36..40].try_into().ok()?);

        Some(Self {
            page_size,
            page_count,
            freelist_head,
            freelist_count,
            schema_root,
        })
    }

    /// Write the header to a writer (typically the beginning of the db file).
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut buf = [0u8; HEADER_SIZE];
        self.serialize(&mut buf);
        writer.write_all(&buf)
    }

    /// Read the header from a reader.
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; HEADER_SIZE];
        reader.read_exact(&mut buf)?;
        Self::deserialize(&buf).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid database header (bad magic)")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_default_header() {
        let header = FileHeader::default();
        let mut buf = [0u8; HEADER_SIZE];
        header.serialize(&mut buf);
        let decoded = FileHeader::deserialize(&buf).expect("should decode");
        assert_eq!(header, decoded);
    }

    #[test]
    fn roundtrip_custom_header() {
        let header = FileHeader {
            page_size: 8192,
            page_count: 42,
            freelist_head: 5,
            freelist_count: 3,
            schema_root: 1,
        };
        let mut buf = [0u8; HEADER_SIZE];
        header.serialize(&mut buf);
        let decoded = FileHeader::deserialize(&buf).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn bad_magic_returns_none() {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(b"bad!");
        assert!(FileHeader::deserialize(&buf).is_none());
    }

    #[test]
    fn too_short_returns_none() {
        let buf = [0u8; 10];
        assert!(FileHeader::deserialize(&buf).is_none());
    }
}
