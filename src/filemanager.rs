use std::collections::HashMap;
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

pub struct BlockId {
    file_name: String,
    block_num: u32,
}

impl BlockId {
    pub fn new(file_name: &str, block_num: u32) -> BlockId {
        BlockId { file_name: file_name.to_string(), block_num }
    }

    pub fn file_name(&self) -> String {
        self.file_name.clone()
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
        write!(f, "{}", format!("[file {}, block number {}]", self.file_name(), self.block_num()))
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

    pub fn get_bytes(&self, offset: u32) -> Option<Box<[u8]>> {
        let bytes = match self.byte_buffer.get(offset as usize..) {
            Some(bytes) => bytes,
            None => return None,
        };

        Some(bytes.into())
    }

    pub fn get_string(&self, offset: u32) -> Option<String> {
        let bytes = match self.byte_buffer.get(offset as usize..) {
            Some(bytes) => bytes,
            None => return None,
        };

        Some(String::from_utf8(bytes.to_vec()).ok().unwrap().trim_end_matches('\0').to_string())
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

    pub fn with_buffer(&mut self) -> &mut Self {
        self.byte_buffer = vec!(0; self.block_size as usize);
        self
    }

    pub fn with_log_buffer(&mut self, buffer: Vec<u8>) -> &mut Self {
        if buffer.len() > self.block_size as usize {
            panic!("buffer cannot exceed block size");
        }
        self.byte_buffer = buffer;
        self
    }

    pub fn build(&mut self) -> Page {
        let bb = std::mem::take(&mut self.byte_buffer);
        Page { block_size: self.block_size, byte_buffer: bb }
    }
}

struct FileManagerStats {
    blocks_read: u64,
    blocks_write: u64
}

impl FileManagerStats {
    pub fn new() -> FileManagerStats {
        FileManagerStats { blocks_read: 0, blocks_write: 0 }
    }

    pub fn blocks_read(&self) -> u64 {
        self.blocks_read
    }

    pub fn blocks_write(&self) -> u64 {
        self.blocks_write
    }

    pub fn set_blocks_read(&mut self, count: u64) {
        self.blocks_read = count;
    }

    pub fn set_blocks_write(&mut self, count: u64) {
        self.blocks_write = count;
    }
}

struct FileManager {
    db_directory: PathBuf,
    block_size: u32,
    is_new: bool,
    open_file: HashMap<String, File>,
    stats: Option<FileManagerStats>,
}

impl FileManager {
    pub fn new(db_directory: PathBuf, block_size: u32) -> FileManager {
        if !db_directory.is_dir() {
            panic!("Database directory is not a directory!");
        }
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

        FileManager { db_directory, block_size, is_new, open_file: HashMap::new(), stats: None }
    }

    pub fn with_stats(&mut self) {
        self.stats = Some(FileManagerStats::new());
    }

    pub fn read(&mut self, block_id: &BlockId, page: &mut Page) -> Result<(), std::io::Error> {
        let mut file = self.open_file(self.db_directory.join(&block_id.file_name()));
        file.seek(std::io::SeekFrom::Start((page.block_size * block_id.block_num()) as u64)).expect("seek error while reading file");
        file.read(page.byte_buffer.as_mut_slice())?;

        Ok(())
    }

    pub fn write(&mut self, block_id: &BlockId, page: &mut Page) -> Result<(), std::io::Error> {
        let mut file = self.open_file(self.db_directory.join(&block_id.file_name()));
        file.seek(std::io::SeekFrom::Start((page.block_size * block_id.block_num()) as u64)).expect("seek error while reading file");
        file.write(page.byte_buffer.as_mut_slice())?;

        Ok(())
    }

    pub fn append(&mut self, file_name: String) -> BlockId {
        let path = self.db_directory.join(&file_name);
        let mut file = self.open_file(path);
        let block_number = (file.metadata().expect("failed to get metadata").len() as u32 / self.block_size);

        file.seek(std::io::SeekFrom::End((self.block_size * block_number) as i64)).expect("seek error");
        let bytes = vec!(0; self.block_size as usize);
        file.write(bytes.as_slice()).expect("failed to write file");

        BlockId::new(&file_name, block_number)
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

    fn open_file(&mut self, file_name: PathBuf) -> File {
        let filename = file_name.to_str().unwrap().to_string();
        match self.open_file.get(filename.as_str()) {
            None => {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(file_name)
                    .expect("failed to open file");
                self.open_file.insert(filename.clone(), file.try_clone().expect("failed to clone file"));
                file
            }
            Some(file) => {
                file.try_clone().expect("failed to clone file")
            }
        }
    }
}

mod tests {
    use tempdir::TempDir;
    use super::*;
    const TEST_BLOCK_SIZE: u32 = 10;
    #[test]
    fn test_block_id() {
        let bid = BlockId::new("test.file", 0);
        assert_eq!(bid.file_name(), "test.file".to_string());
        assert_eq!(bid.block_num(), 0);
        let bid2= BlockId::new("test.file", 0);
        assert!(bid == bid2);
    }

    #[test]
    fn test_page() {
        let mut page = Page::builder().block_size(TEST_BLOCK_SIZE).with_buffer().build();
        assert_eq!(page.block_size(), TEST_BLOCK_SIZE);
        assert_eq!(page.get_int(0), Some(0));
        page.set_int(0, Some(65));
        assert_eq!(page.get_int(0), Some(65));
        let expected = b"A\0\0\0\0\0\0\0\0\0".to_vec().into_boxed_slice();
        assert_eq!(page.get_bytes(0), Some(expected));
        page.set_bytes(1, Some(b"B"));
        let expected = b"AB\0\0\0\0\0\0\0\0".to_vec().into_boxed_slice();
        assert_eq!(page.get_bytes(0), Some(expected));
        page.set_bytes(0, Some(b"foobar"));
        let expected = b"foobar\0\0\0\0".to_vec().into_boxed_slice();
        assert_eq!(page.get_bytes(0), Some(expected));
        assert_eq!(page.get_string(0), Some(String::from_utf8(b"foobar".to_vec()).expect("foobar is utf8")));

        assert_eq!(page.get_bytes(12), None);
        assert_eq!(page.get_int(12), None);
        assert_eq!(page.get_string(12), None);
    }

    #[test]
    fn test_file_manager() {
        let tmp_dir = TempDir::new("test_file_manager").expect("failed to create temp dir");
        let mut file_manager = FileManager::new(tmp_dir.path().to_owned(), TEST_BLOCK_SIZE);
        assert_eq!(file_manager.is_new(), true);
        let blid = file_manager.append(String::from("test.block"));
        assert_eq!(blid.block_num(), 0);

        let mut page = Page::builder().block_size(TEST_BLOCK_SIZE).with_buffer().build();
        assert_eq!(page.block_size(), 10);
        assert_eq!(page.get_bytes(0), Some(b"\0\0\0\0\0\0\0\0\0\0".to_vec().into_boxed_slice()));
        page.set_bytes(0, Some(b"B"));
        assert_eq!(page.get_bytes(0), Some(b"B\0\0\0\0\0\0\0\0\0".to_vec().into_boxed_slice()));
        file_manager.write(&blid, &mut page).expect("failed to write file");

        let mut page2 = Page::builder().block_size(TEST_BLOCK_SIZE).with_buffer().build();
        assert_eq!(page2.get_bytes(0), Some(b"\0\0\0\0\0\0\0\0\0\0".to_vec().into_boxed_slice()));
        file_manager.read(&blid, &mut page2).expect("failed to read file");
        assert_eq!(page2.get_bytes(0), Some(b"B\0\0\0\0\0\0\0\0\0".to_vec().into_boxed_slice()));

        let blid2 = file_manager.append(String::from("test.block"));
        assert_eq!(blid2.block_num, 1);
        file_manager.write(&blid2, &mut page).expect("failed to write file");
    }
}
