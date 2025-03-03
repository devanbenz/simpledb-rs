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
    pub fn new(log_file: String, mut file_manager: FileManager) -> Self {
        let buf: Vec<u8> = vec![0; file_manager.block_size()];
        let block_id = match file_manager.length(&log_file) {
            Ok(file_len) => {
                let blid = BlockId::new(&log_file, (file_len - 1));
                let mut log_page = PageBuilder::new().with_log_buffer(buf).build();
                file_manager
                    .read(&blid, &mut log_page)
                    .expect("error reading blockId in to log page");
                blid
            }
            Err(_) => {
                Self::append_new_block()
                file_manager.append(&log_file)
            },
        };
        Self {
            log_file,
            file_manager,
            log_page,
            block_id,
            latest_lsn: 0,
            last_lsn: 0,
        }
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

    fn append_new_block(&mut self) -> BlockId {
        let blid = self.file_manager.append(&self.log_file);
        let log_page = PageBuilder::new().with_log_buffer(vec!(0; self.file_manager.block_size())).build();
        self.log_page = log_page;

        self.log_page
            .set_int(0, Some(self.file_manager.block_size() as i32));
        self.file_manager
            .write(&blid, &mut self.log_page)
            .expect("error writing to log file");
        blid
    }
}

struct LogManagerBuilder {
    log_file: String,
    file_manager: FileManager
}

impl LogManagerBuilder {
    fn new(log_file: String, file_manager: FileManager) -> Self {
        Self {log_file, file_manager}
    }

    pub fn build(self) -> LogManager {
        LogManager::new(self.log_file, self.file_manager)
    }
}
