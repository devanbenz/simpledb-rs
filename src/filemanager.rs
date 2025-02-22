use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::{is_separator, Path, PathBuf};

pub struct BlockId {
    file_name: String,
    block_num: u32,
}

impl BlockId {
    pub fn new(file_name: &str, block_num: u32) -> BlockId {
        BlockId { file_name: file_name.to_string(), block_num }
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    pub fn block_num(&self) -> u32 {
        self.block_num
    }
}

impl PartialEq for BlockId {
    fn eq(&self, other: &Self) -> bool {
        self.file_name == other.file_name && self.block_num == other.block_num
    }
}

impl Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("[file {}, block number {}]", self.file_name, self.block_num))
    }
}

struct Page {
    block_size: u32,
    byte_buffer: Vec<u8>,
}

impl Page {
    pub fn builder() -> PageBuilder {
        PageBuilder::new()
    }

    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    pub fn get_int(&self, offset: u32) -> Option<u8> {
        self.byte_buffer.get(offset as usize).copied()
    }

    pub fn get_bytes(&self, offset: u32) -> Option<&[u8]> {
        self.byte_buffer.get(offset as usize..)
    }

    pub fn get_string(&self, offset: u32) -> Option<String> {
        String::from_utf8(self.byte_buffer[offset as usize..].to_vec()).ok()
    }

    pub fn set_int(&mut self, offset: u32, val: Option<u8>) {
        if let Some(val) = val {
            self.byte_buffer[offset as usize] = val;
        }
    }

    pub fn set_bytes(&mut self, offset: u32, bytes: Option<&[u8]>) {
        if let Some(bytes) = bytes {
            self.byte_buffer[offset as usize..offset as usize + bytes.len()].copy_from_slice(bytes);
        }
    }

    pub fn set_string(&mut self, offset: u32, val: Option<String>) {
        if let Some(val) = val {
            self.byte_buffer[offset as usize..offset as usize + val.len()].copy_from_slice(val.as_bytes());
        }
    }

    pub(crate) fn contents(&self) -> Vec<u8> {
        self.byte_buffer.clone()
    }
}

pub struct PageBuilder {
    block_size: u32,
    byte_buffer: Vec<u8>,
}

impl PageBuilder {
    pub fn new() -> PageBuilder {
        PageBuilder { block_size: 0, byte_buffer: Vec::new() }
    }

    pub fn block_size(&mut self, block_size: u32) -> &mut Self {
        self.block_size = block_size;
        self
    }

    pub fn buffer(&mut self, block_size: u32) -> &mut Self {
        self.byte_buffer = vec!(0; block_size as usize);
        self
    }

    pub fn log_buffer(&mut self, buffer: Vec<u8>) -> &mut Self {
        self.byte_buffer = buffer;
        self
    }

    pub fn build(&mut self) -> Page {
        let bb = std::mem::take(&mut self.byte_buffer);
        Page { block_size: self.block_size, byte_buffer: bb }
    }
}

struct FileManager {
    db_directory: PathBuf,
    block_size: u32,
    is_new: bool,
    open_file: HashMap<String, File>,
}

impl FileManager {
    pub fn new(db_directory: PathBuf, block_size: u32) -> FileManager {
        let is_new = std::fs::exists(&db_directory).unwrap_or(false);
        let files = std::fs::read_dir(&db_directory).expect("failed to read directory");

        // Remove all temp files on startup
        for file in files {
            if let Ok(file) = file {
                if !file.file_name().into_string().unwrap().starts_with("temp") {
                    continue;
                } else {
                    std::fs::remove_file(file.path()).expect("failed to remove file");
                }
            }
        }

        FileManager { db_directory, block_size, is_new, open_file: HashMap::new() }
    }

    pub fn read(&self, block_id: &BlockId, page: &mut Page) -> Result<(), std::io::Error> {
        let mut file = self.open_file.get(block_id.file_name());
        if let Some(mut file) = file {
            file.seek(std::io::SeekFrom::Start(page.block_size as u64)).expect("seek error while reading file");
            file.read(page.contents().as_mut_slice())?;

            Ok(())
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "cannot read file with provided block id"))
        }
    }

    pub fn write(&self, block_id: &BlockId, page: &mut Page) -> Result<(), std::io::Error> {
        let mut file = self.open_file.get(block_id.file_name());
        if let Some(mut file) = file {
            file.seek(std::io::SeekFrom::Start(page.block_size as u64)).expect("seek error while reading file");
            file.write(page.contents().as_mut_slice())?;

            Ok(())
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "cannot read file with provided block id"))
        }
    }

    pub fn append(&self, file_name: &str) -> BlockId {
        todo!()
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    pub fn length(&self, file_name: &str) -> Result<usize, std::io::Error> {
        let file = self.open_file.get(file_name);
        if let Some(file) = file {
            Ok(file.metadata()?.len() as usize)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::NotFound, "file not found"))
        }
    }

    pub fn block_size(&self) -> usize {
        self.block_size as usize
    }
}

mod tests {
    use tempdir::TempDir;
    use super::*;
    #[test]
    fn test_block_id() {
        let bid = BlockId::new("test.file", 10);
        assert_eq!(bid.file_name(), "test.file");
        assert_eq!(bid.block_num(), 10);
        let bid2= BlockId::new("test.file", 10);
        assert!(bid == bid2);
    }

    #[test]
    fn test_page() {}

    #[test]
    fn test_file_manager() {}
}
