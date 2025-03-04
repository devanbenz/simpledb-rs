use crate::filemanager::{BlockId, FileManager, Page, PageBuilder};
use std::io::Error;

pub struct LogManager {
    log_file: String,
    file_manager: FileManager,
    log_page: Page,
    block_id: BlockId,
    latest_lsn: i32,
    last_lsn: i32,
}

impl Iterator for LogManager {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl LogManager {
    pub fn builder(log_file: String, mut file_manager: FileManager) -> LogManagerBuilder {
        LogManagerBuilder::new(log_file, file_manager)
    }

    pub fn append(&mut self, rec: Vec<u8>) -> i32 {
        let reclen = rec.len();
        let bytes_needed = reclen + size_of::<i32>();
        if let Some(boundary) = self.log_page.get_int(0) {
            if (boundary as usize - bytes_needed) < size_of::<i32>() {
            } else {
            }
        } else {
            panic!("no page available")
        }

        self.latest_lsn
    }

    pub fn flush(&mut self, lsn: i32) {
        if lsn >= self.last_lsn {
            self.flush_to_file()
        }
    }

    fn flush_to_file(&mut self) {
        self.file_manager
            .write(&self.block_id, &mut self.log_page)
            .expect("error writing to log file");
        self.last_lsn = self.latest_lsn;
    }
}

struct LogManagerBuilder {
    log_file: String,
    file_manager: FileManager,
    log_page: Page,
}

impl LogManagerBuilder {
    pub fn new(log_file: String, file_manager: FileManager) -> Self {
        let mut page = PageBuilder::new().with_log_buffer(vec!(0; file_manager.block_size())).build();
        Self {log_file, file_manager, log_page: page }
    }

    pub fn build(mut self) -> LogManager {
        let blid = match self.file_manager.length(&self.log_file) {
            None => self.append_new_block(),
            Some(file_len) => {
                if file_len > 0 {
                    let blid = BlockId::new(&self.log_file, (file_len - 1));
                    self.file_manager.read(&blid, &mut self.log_page).expect("could not read block id in to page");
                    blid
                } else {
                    self.append_new_block()
                }
            }
        };

        LogManager {
            log_file: self.log_file,
            file_manager: self.file_manager,
            log_page: self.log_page,
            block_id: blid,
            latest_lsn: 0,
            last_lsn: 0
        }
    }

    fn append_new_block(&mut self) -> BlockId {
        let blid = self.file_manager.append(&self.log_file);
        self.log_page.set_int(0, Some(self.file_manager.block_size() as i32));
        self.file_manager.write(&blid, &mut self.log_page).expect("could not write block id in to log file");
        blid
    }
}

mod tests {
    use tempdir::TempDir;
    use super::*;
    const TEST_BLOCK_SIZE: usize = 4;
   #[test]
   fn test_log_manger() {
       let tmp_dir = TempDir::new("test_log_manager").expect("failed to create temp dir");
       let file_manager = FileManager::new(tmp_dir.path().to_owned(), TEST_BLOCK_SIZE);
       let log_manager = LogManager::builder("log.wal".to_string(), file_manager).build();
       assert_eq!(log_manager.block_id.block_num(), 0);
   }
}
