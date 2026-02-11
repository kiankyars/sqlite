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
    page_size: usize,
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
        Ok(Self {
            file,
            page_size: page_size as usize,
        })
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

    pub(crate) fn recover(&mut self, db_file: &mut File, db_page_size: usize) -> io::Result<usize> {
        let committed_pages = self.read_committed_pages(db_page_size)?;
        if !committed_pages.is_empty() {
            apply_pages_to_db(db_file, db_page_size, &committed_pages)?;
            db_file.sync_all()?;
        }

        self.reset()?;
        Ok(committed_pages.len())
    }

    pub(crate) fn checkpoint(
        &mut self,
        db_file: &mut File,
        db_page_size: usize,
    ) -> io::Result<usize> {
        self.recover(db_file, db_page_size)
    }

    fn reset(&mut self) -> io::Result<()> {
        self.file.set_len(WAL_HEADER_SIZE as u64)?;
        self.file.seek(SeekFrom::End(0))?;
        self.file.sync_all()?;
        Ok(())
    }

    fn read_committed_pages(&mut self, db_page_size: usize) -> io::Result<Vec<(PageNum, Vec<u8>)>> {
        if db_page_size != self.page_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "database page size {} does not match WAL page size {}",
                    db_page_size, self.page_size
                ),
            ));
        }

        self.file.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        self.file.read_to_end(&mut bytes)?;

        if bytes.len() < WAL_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL file smaller than header",
            ));
        }
        if &bytes[0..8] != WAL_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid WAL header (bad magic)",
            ));
        }
        let version = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        if version != WAL_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported WAL version {version}"),
            ));
        }
        let wal_page_size = u32::from_be_bytes(bytes[12..16].try_into().unwrap()) as usize;
        if wal_page_size != db_page_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "WAL page size {} does not match database page size {}",
                    wal_page_size, db_page_size
                ),
            ));
        }

        let mut offset = WAL_HEADER_SIZE;
        let mut pending_txn: Option<u64> = None;
        let mut pending_pages: Vec<(PageNum, Vec<u8>)> = Vec::new();
        let mut committed_pages: Vec<(PageNum, Vec<u8>)> = Vec::new();

        while offset < bytes.len() {
            let frame_type = bytes[offset];
            offset += 1;

            match frame_type {
                FRAME_TYPE_PAGE => {
                    if bytes.len().saturating_sub(offset) < 8 + 4 + 4 + 4 {
                        break;
                    }
                    let txn_id = u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    let page_num =
                        u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
                    offset += 4;
                    let payload_len =
                        u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    let checksum =
                        u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
                    offset += 4;

                    if payload_len != self.page_size {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "WAL frame payload size {} does not match page size {}",
                                payload_len, self.page_size
                            ),
                        ));
                    }
                    if bytes.len().saturating_sub(offset) < payload_len {
                        break;
                    }
                    let payload = bytes[offset..offset + payload_len].to_vec();
                    offset += payload_len;

                    let mut frame_header = Vec::with_capacity(1 + 8 + 4 + 4);
                    frame_header.push(FRAME_TYPE_PAGE);
                    frame_header.extend_from_slice(&txn_id.to_be_bytes());
                    frame_header.extend_from_slice(&page_num.to_be_bytes());
                    frame_header.extend_from_slice(&(payload_len as u32).to_be_bytes());
                    if checksum != checksum32(&[&frame_header, &payload]) {
                        break;
                    }

                    if pending_txn != Some(txn_id) {
                        pending_txn = Some(txn_id);
                        pending_pages.clear();
                    }
                    pending_pages.push((page_num, payload));
                }
                FRAME_TYPE_COMMIT => {
                    if bytes.len().saturating_sub(offset) < 8 + 4 + 4 {
                        break;
                    }
                    let txn_id = u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
                    offset += 8;
                    let frame_count =
                        u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                    offset += 4;
                    let checksum =
                        u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
                    offset += 4;

                    let mut commit_header = Vec::with_capacity(1 + 8 + 4);
                    commit_header.push(FRAME_TYPE_COMMIT);
                    commit_header.extend_from_slice(&txn_id.to_be_bytes());
                    commit_header.extend_from_slice(&(frame_count as u32).to_be_bytes());
                    if checksum != checksum32(&[&commit_header]) {
                        break;
                    }

                    if pending_txn == Some(txn_id) && frame_count == pending_pages.len() {
                        committed_pages.append(&mut pending_pages);
                    } else {
                        pending_pages.clear();
                    }
                    pending_txn = None;
                }
                _ => break,
            }
        }

        self.file.seek(SeekFrom::End(0))?;
        Ok(committed_pages)
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

fn apply_pages_to_db(
    db_file: &mut File,
    page_size: usize,
    pages: &[(PageNum, Vec<u8>)],
) -> io::Result<()> {
    for (page_num, payload) in pages {
        if payload.len() != page_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "WAL frame payload size {} does not match page size {}",
                    payload.len(),
                    page_size
                ),
            ));
        }
        let offset = *page_num as u64 * page_size as u64;
        db_file.seek(SeekFrom::Start(offset))?;
        db_file.write_all(payload)?;
    }
    Ok(())
}
