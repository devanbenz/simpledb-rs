use crate::filemanager::{BlockId, FileManager, Page, PageBuilder};
use std::cell::{RefCell, RefMut};
use std::ops::Add;
use std::rc::Rc;

pub struct LogIterator {
    file_manager: Rc<RefCell<FileManager>>,
    log_page: Page,
    block_id: BlockId,
    current_offset: i32,
}

impl LogIterator {
    pub fn new(fm: Rc<RefCell<FileManager>>, blk: BlockId) -> Self {
        let fm_mut = fm.borrow_mut();
        let b = vec![0; fm_mut.block_size()];
        let mut p = Page::builder()
            .block_size(fm_mut.block_size())
            .with_log_buffer(b)
            .build();
        let current_b = Self::move_to_block(fm_mut, &blk, &mut p);
        Self {
            file_manager: fm,
            log_page: p,
            block_id: blk,
            current_offset: current_b,
        }
    }

    fn move_to_block(mut fm: RefMut<FileManager>, blk: &BlockId, lp: &mut Page) -> i32 {
        fm.read(blk, lp).expect("could not read block in to page");
        let boundary = lp.get_int(0).expect("could not read boundary in page");
        boundary
    }
}

impl Iterator for LogIterator {
    type Item = Box<[u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset >= self.file_manager.borrow_mut().block_size() as i32
            || self.block_id.block_num() > 0
        {
            return None;
        }
        let bytes = self.log_page.get_bytes(self.current_offset as usize);
        if bytes == None {
            return None;
        }

        let total = self
            .current_offset
            .add((size_of::<i32>() + bytes.as_ref()?.len()) as i32);
        self.current_offset = total;

        bytes
    }
}

pub struct LogManager {
    log_file: String,
    file_manager: Rc<RefCell<FileManager>>,
    log_page: Page,
    block_id: BlockId,
    latest_lsn: i32,
    last_lsn: i32,
}

impl LogManager {
    pub fn builder(log_file: String, file_manager: Rc<RefCell<FileManager>>) -> LogManagerBuilder {
        LogManagerBuilder::new(log_file, file_manager)
    }

    pub fn append(&mut self, rec: Vec<u8>) -> i32 {
        let reclen = rec.len();
        let bytes_needed = reclen + size_of::<i32>();
        if let Some(b) = self.log_page.get_int(0) {
            let boundary;
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
            .borrow_mut()
            .write(&self.block_id, &mut self.log_page)
            .expect("error writing to log file");
        self.last_lsn = self.latest_lsn;
    }

    fn append_new_block(&mut self) -> BlockId {
        let blid = self.file_manager.borrow_mut().append(&self.log_file);
        self.log_page.flush();
        self.log_page
            .set_int(0, Some(self.file_manager.borrow().block_size() as i32));
        self.file_manager
            .borrow_mut()
            .write(&blid, &mut self.log_page)
            .expect("could not write block id in to log file");
        blid
    }
}

pub struct LogManagerBuilder {
    log_file: String,
    file_manager: Rc<RefCell<FileManager>>,
    log_page: Page,
}

impl LogManagerBuilder {
    pub fn new(log_file: String, file_manager: Rc<RefCell<FileManager>>) -> Self {
        let page = PageBuilder::new()
            .with_log_buffer(vec![0; file_manager.borrow().block_size()])
            .build();
        Self {
            log_file,
            file_manager,
            log_page: page,
        }
    }

    pub fn build(mut self) -> LogManager {
        let fm = self.file_manager.clone();
        let file_len = {
            let blid = fm.borrow_mut().length(&self.log_file);
            blid
        };

        let blid = {
            if file_len == None {
                self.append_new_block()
            } else {
                let file_len = file_len.expect("no block in log file");
                if file_len > 0 {
                    let blid = BlockId::new(&self.log_file, file_len - 1);
                    self.file_manager
                        .borrow_mut()
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
        let blid = self.file_manager.borrow_mut().append(&self.log_file);
        self.log_page
            .set_int(0, Some(self.file_manager.borrow_mut().block_size() as i32));
        self.file_manager
            .borrow_mut()
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
        let file_manager = Rc::new(RefCell::new(FileManager::new(
            tmp_dir.path().to_owned(),
            TEST_BLOCK_SIZE,
        )));
        let log_manager = LogManager::builder("log.wal".to_string(), file_manager).build();
        assert_eq!(log_manager.block_id.block_num(), 0);
        assert_eq!(log_manager.latest_lsn, 0);
        assert_eq!(log_manager.last_lsn, 0);
        assert_eq!(
            log_manager.log_page.get_int(0),
            Some(TEST_BLOCK_SIZE as i32)
        );
        tmp_dir.close().expect("failed to remove temp dir");
    }

    #[test]
    fn test_log_manger_append() {
        let tmp_dir = TempDir::new("test_log_manager").expect("failed to create temp dir");
        let file_manager = Rc::new(RefCell::new(FileManager::new(
            tmp_dir.path().to_owned(),
            TEST_BLOCK_SIZE,
        )));
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
        tmp_dir.close().expect("failed to remove temp dir");
    }

    #[test]
    fn test_log_iterator() {
        let tmp_dir = TempDir::new("test_log_manager").expect("failed to create temp dir");
        let file_manager = Rc::new(RefCell::new(FileManager::new(
            tmp_dir.path().to_owned(),
            TEST_BLOCK_SIZE,
        )));
        let log_manager = Rc::new(RefCell::new(
            LogManager::builder("log.wal".to_string(), file_manager.clone()).build(),
        ));
        let initial_block_id = {
            let mut lm = log_manager.borrow_mut();
            lm.append("foo".as_bytes().to_vec());
            lm.append("bar".as_bytes().to_vec());
            lm.flush();
            // First block ID
            BlockId::new(&lm.log_file, 0)
        };

        let mut log_iterator = LogIterator::new(file_manager.clone(), initial_block_id);
        let first = log_iterator.next();
        assert!(first.is_some());
        assert_eq!(first.unwrap().to_owned().to_vec(), vec![98, 97, 114]);
        let second = log_iterator.next();
        assert!(second.is_some());
        assert_eq!(log_iterator.next(), None);
        tmp_dir.close().expect("failed to remove temp dir");
    }
}
