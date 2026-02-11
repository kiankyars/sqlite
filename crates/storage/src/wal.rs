use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::pager::PageNum;

pub(crate) const WAL_MAGIC: &[u8; 8] = b"RSQLWAL1";
pub(crate) const WAL_VERSION: u32 = 1;
pub(crate) const WAL_HEADER_SIZE: usize = 16;

const FRAME_TYPE_PAGE: u8 = 1;
const FRAME_TYPE_COMMIT: u8 = 2;

pub(crate) struct Wal {
    file: File,
}

impl Wal {
    pub(crate) fn open(db_path: &Path, page_size: u32) -> io::Result<Self> {
        let wal_path = wal_path_for(db_path);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&wal_path)?;

        let len = file.metadata()?.len();
        if len == 0 {
            write_header(&mut file, page_size)?;
            file.sync_all()?;
        } else {
            verify_header(&mut file, page_size)?;
        }

        file.seek(SeekFrom::End(0))?;
        Ok(Self { file })
    }

    pub(crate) fn append_txn(
        &mut self,
        txn_id: u64,
        pages: &[(PageNum, Vec<u8>)],
    ) -> io::Result<()> {
        self.file.seek(SeekFrom::End(0))?;

        for (page_num, payload) in pages {
            let payload_len = u32::try_from(payload.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "page payload too large for WAL",
                )
            })?;
            let mut header = Vec::with_capacity(1 + 8 + 4 + 4);
            header.push(FRAME_TYPE_PAGE);
            header.extend_from_slice(&txn_id.to_be_bytes());
            header.extend_from_slice(&page_num.to_be_bytes());
            header.extend_from_slice(&payload_len.to_be_bytes());
            let checksum = checksum32(&[&header, payload]);

            self.file.write_all(&header)?;
            self.file.write_all(&checksum.to_be_bytes())?;
            self.file.write_all(payload)?;
        }

        let mut commit_header = Vec::with_capacity(1 + 8 + 4);
        commit_header.push(FRAME_TYPE_COMMIT);
        commit_header.extend_from_slice(&txn_id.to_be_bytes());
        commit_header.extend_from_slice(
            &(u32::try_from(pages.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "too many WAL frames in transaction",
                )
            })?)
            .to_be_bytes(),
        );
        let commit_checksum = checksum32(&[&commit_header]);
        self.file.write_all(&commit_header)?;
        self.file.write_all(&commit_checksum.to_be_bytes())?;

        self.file.sync_all()?;
        Ok(())
    }
}

pub(crate) fn wal_path_for(db_path: &Path) -> PathBuf {
    let mut wal_os: OsString = db_path.as_os_str().to_os_string();
    wal_os.push("-wal");
    PathBuf::from(wal_os)
}

pub(crate) fn checksum32(parts: &[&[u8]]) -> u32 {
    let mut hash: u32 = 0x811c9dc5;
    for part in parts {
        for byte in *part {
            hash ^= u32::from(*byte);
            hash = hash.wrapping_mul(0x0100_0193);
        }
    }
    hash
}

fn write_header(file: &mut File, page_size: u32) -> io::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    file.write_all(WAL_MAGIC)?;
    file.write_all(&WAL_VERSION.to_be_bytes())?;
    file.write_all(&page_size.to_be_bytes())?;
    Ok(())
}

fn verify_header(file: &mut File, expected_page_size: u32) -> io::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; WAL_HEADER_SIZE];
    file.read_exact(&mut header)?;

    if &header[0..8] != WAL_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid WAL header (bad magic)",
        ));
    }

    let version = u32::from_be_bytes(header[8..12].try_into().unwrap());
    if version != WAL_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported WAL version {version}"),
        ));
    }

    let page_size = u32::from_be_bytes(header[12..16].try_into().unwrap());
    if page_size != expected_page_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "WAL page size {} does not match database page size {}",
                page_size, expected_page_size
            ),
        ));
    }

    Ok(())
}
