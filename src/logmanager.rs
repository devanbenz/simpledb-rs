use std::rc::Rc;
use crate::filemanager::{BlockId, FileManager, Page, PageBuilder};

pub struct LogIterator {
    file_manager: Rc<FileManager>,
    log_page: Page,
    block_id: BlockId,
    current_offset: i32,
    log_boundary: i32,
}

impl LogIterator {
    pub fn new(mut fm: Rc<FileManager>, blk: BlockId) -> Self {
        let b = vec![0; fm.block_size()];
        let mut p = Page::builder()
            .block_size(fm.block_size())
            .with_log_buffer(b)
            .build();
        let current_b = Self::move_to_block(&mut fm, &blk, &mut p);
        Self {
            file_manager: fm,
            log_page: p,
            block_id: blk,
            current_offset: current_b,
            log_boundary: current_b,
        }
    }

    fn move_to_block(fm: &mut FileManager, blk: &BlockId, lp: &mut Page) -> i32 {
        fm.read(blk, lp).expect("could not read block in to page");
        let boundary = lp.get_int(0).expect("could not read boundary in page");
        boundary
    }
}

impl Iterator for LogIterator {
    type Item = Box<[u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut blk_size = self
            .log_page
            .get_int(self.current_offset as usize)
            .expect("could not read block size in page");
        if blk_size == self.log_boundary {
            let next_block_id =
                BlockId::new(&self.block_id.file_name(), &self.block_id.block_num() + 1);
            match self.file_manager.read(&next_block_id, &mut self.log_page) {
                Err(_) => {
                    return None;
                }
                Ok(_) => {}
            }
            blk_size =
                Self::move_to_block(&mut self.file_manager, &self.block_id, &mut self.log_page);
        }
        if blk_size == 0 {
            return None;
        }

        self.log_page.get_bytes(blk_size as usize)
    }
}

pub struct LogManager {
    log_file: String,
    file_manager: Rc<FileManager>,
    log_page: Page,
    block_id: BlockId,
    latest_lsn: i32,
    last_lsn: i32,
}

impl LogManager {
    pub fn builder(log_file: String, mut file_manager: Rc<FileManager>) -> LogManagerBuilder {
        LogManagerBuilder::new(log_file, file_manager)
    }

    pub fn append(&mut self, rec: Vec<u8>) -> i32 {
        let reclen = rec.len();
        let bytes_needed = reclen + size_of::<i32>();
        let mut boundary = 0;
        if let Some(b) = self.log_page.get_int(0) {
            if (b as usize - bytes_needed) < size_of::<i32>() {
                self.flush();
                self.block_id = self.append_new_block();
                boundary = self.log_page.get_int(0).expect("failed to get int");
            } else {
                boundary = b;
            }
            let recpos = boundary as usize - bytes_needed;
            self.log_page.set_bytes(recpos, Some(rec.as_slice()));
            self.log_page.set_int(0, Some(recpos as i32));
            self.latest_lsn += 1;

            self.latest_lsn
        } else {
            panic!("no page available")
        }
    }

    pub fn flush(&mut self) {
        if self.latest_lsn >= self.last_lsn {
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
        self.log_page.flush();
        self.log_page
            .set_int(0, Some(self.file_manager.block_size() as i32));
        self.file_manager
            .write(&blid, &mut self.log_page)
            .expect("could not write block id in to log file");
        blid
    }
}

struct LogManagerBuilder {
    log_file: String,
    file_manager: Rc<FileManager>,
    log_page: Page,
}

impl LogManagerBuilder {
    pub fn new(log_file: String, file_manager: Rc<FileManager>) -> Self {
        let mut page = PageBuilder::new()
            .with_log_buffer(vec![0; file_manager.block_size()])
            .build();
        Self {
            log_file,
            file_manager,
            log_page: page,
        }
    }

    pub fn build(mut self) -> LogManager {
        let blid = match self.file_manager.length(&self.log_file) {
            None => self.append_new_block(),
            Some(file_len) => {
                if file_len > 0 {
                    let blid = BlockId::new(&self.log_file, (file_len - 1));
                    self.file_manager
                        .read(&blid, &mut self.log_page)
                        .expect("could not read block id in to page");
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
            last_lsn: 0,
        }
    }

    fn append_new_block(&mut self) -> BlockId {
        let blid = self.file_manager.append(&self.log_file);
        self.log_page
            .set_int(0, Some(self.file_manager.block_size() as i32));
        self.file_manager
            .write(&blid, &mut self.log_page)
            .expect("could not write block id in to log file");
        blid
    }
}

mod tests {
    use super::*;
    use std::rc::Rc;
    use tempdir::TempDir;
    const TEST_BLOCK_SIZE: usize = 4 * 8;
    #[test]
    fn test_log_manger_builder() {
        let tmp_dir = TempDir::new("test_log_manager").expect("failed to create temp dir");
        let file_manager = Rc::new(FileManager::new(tmp_dir.path().to_owned(), TEST_BLOCK_SIZE));
        let log_manager = LogManager::builder("log.wal".to_string(), file_manager).build();
        assert_eq!(log_manager.block_id.block_num(), 0);
        assert_eq!(log_manager.latest_lsn, 0);
        assert_eq!(log_manager.last_lsn, 0);
        assert_eq!(
            log_manager.log_page.get_int(0),
            Some(TEST_BLOCK_SIZE as i32)
        );
    }

    #[test]
    fn test_log_manger_append() {
        let tmp_dir = TempDir::new("test_log_manager").expect("failed to create temp dir");
        let file_manager = Rc::new(FileManager::new(tmp_dir.path().to_owned(), TEST_BLOCK_SIZE));
        let mut log_manager = LogManager::builder("log.wal".to_string(), file_manager).build();
        assert_eq!(log_manager.block_id.block_num(), 0);
        assert_eq!(log_manager.latest_lsn, 0);
        assert_eq!(log_manager.last_lsn, 0);
        assert_eq!(
            log_manager.log_page.get_int(0),
            Some(TEST_BLOCK_SIZE as i32)
        );

        log_manager.append("foo".as_bytes().to_vec());
        assert_eq!(log_manager.latest_lsn, 1);
        log_manager.append("bar".as_bytes().to_vec());
        assert_eq!(log_manager.latest_lsn, 2);
        log_manager.append("fizz".as_bytes().to_vec());
        assert_eq!(log_manager.latest_lsn, 3);
        // This append will flush the log page to disk
        log_manager.append("buzz".as_bytes().to_vec());
        assert_eq!(log_manager.latest_lsn, 4);
    }

    #[test]
    fn test_log_iterator() {
        let tmp_dir = TempDir::new("test_log_manager").expect("failed to create temp dir");
        let file_manager = Rc::new(FileManager::new(tmp_dir.path().to_owned(), TEST_BLOCK_SIZE));
        let mut log_manager = Rc::new(LogManager::builder("log.wal".to_string(), file_manager.clone()).build());
        let mut lm_logit = log_manager.clone();
        let inital_block_id = lm_logit.append_new_block();
        lm_logit.append("foo".as_bytes().to_vec());
        lm_logit.append("bar".as_bytes().to_vec());
        let mut log_iterator = LogIterator::new(lm_logit.file_manager.clone(), inital_block_id);
    }
}
