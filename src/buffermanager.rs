use crate::filemanager::{BlockId, FileManager, Page};
use crate::logmanager::LogManager;
use std::cell::{Ref, RefCell};
use std::fs::File;
use std::rc::Rc;
use std::sync::atomic::{AtomicI32, Ordering};
use log::log;

struct Buffer {
    file_manager: Rc<RefCell<FileManager>>,
    log_manager: Rc<RefCell<LogManager>>,
    block_id: Option<BlockId>,
    contents: Rc<RefCell<Page>>,
    pins: AtomicI32,
    txn: Option<usize>,
    lsn: Option<usize>,
}

impl Buffer {
    pub fn new(
        file_manager: Rc<RefCell<FileManager>>,
        log_manager: Rc<RefCell<LogManager>>,
    ) -> Buffer {
        let fm_blk_size = { file_manager.borrow_mut().block_size() };

        let page = Rc::new(RefCell::new(Page::builder()
            .block_size(fm_blk_size)
            .with_buffer()
            .build()));
        Buffer {
            file_manager,
            log_manager,
            block_id: None,
            contents: page,
            pins: AtomicI32::new(0),
            txn: None,
            lsn: None,
        }
    }

    pub fn contents(&self) -> Rc<RefCell<Page>> {
        let page = self.contents.clone();
        page
    }

    pub fn block_id(&self) -> &Option<BlockId> {
        &self.block_id
    }

    pub fn pinned(&self) -> bool {
        self.pins.load(Ordering::Relaxed) > 0
    }

    pub fn set_modified(&mut self, txn: usize, lsn: usize) {
        self.txn = Some(txn);
        if let Some(lsn) = self.lsn {
            self.lsn = Some(lsn);
        }
    }

    pub fn modifying_txn(&self) -> Option<usize> {
        self.txn
    }

    pub fn pin(&self) {
        self.pins.fetch_add(1, Ordering::Relaxed);
    }

    pub fn unpin(&self) {
       self.pins.fetch_sub(1, Ordering::Relaxed);
    }

    fn flush(&mut self) {
        if let Some(txn) = self.txn {
            self.log_manager.borrow_mut().flush();
            match self.block_id() {
                None => {
                    log::warn!("no block id provided")
                }
                Some(blid) => {
                    let mut page_clone = self.contents.clone();
                    {
                        let page = page_clone.get_mut();
                        self.file_manager.borrow_mut().write(blid, page).expect("could not write to file manager");
                        if (txn == 1) {
                            self.txn = None;
                        } else {
                            self.txn = Some(txn - 1);
                        }
                    }
                }
            }
        }
    }
}

struct BufferManager {
    file_manager: Rc<RefCell<FileManager>>,
    log_manager: Rc<RefCell<LogManager>>,
    buffer_pool: Vec<Buffer>,
    buff_n_available: AtomicI32,
}

impl BufferManager {
    const MAX_TIME: u128 = 1000;

    fn new(file_manager: Rc<RefCell<FileManager>>, log_manager: Rc<RefCell<LogManager>>, buff_n: i32) -> BufferManager {
        let mut buffer_pool = vec![];
        for i in 0..buff_n {
            buffer_pool.push(Buffer::new(file_manager.clone(), log_manager.clone()));
        }
        let buff_n_available = AtomicI32::new(buff_n);

        BufferManager {
            file_manager,
            log_manager,
            buffer_pool,
            buff_n_available,
        }
    }
    pub fn pin(&mut self, block_id: &BlockId) -> Option<&mut Buffer> {
        let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
        loop {
            if let Some(buffer) = self.try_pin(block_id) {
                return Some(buffer);
            } else {
                if self.waiting_too_long(timestamp) {
                    return None;
                }
            }
        }
    }

    pub fn unpin(&mut self, buffer: &mut Buffer) {
        buffer.unpin();
        if (!buffer.pinned()) {
            self.buff_n_available.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn available_buffers(&self) -> i32 {
        self.buff_n_available.load(Ordering::Relaxed)
    }

    pub fn flush_all_buffers(&mut self, txn_num: usize) {
        for buffer in self.buffer_pool.iter_mut() {
            if buffer.modifying_txn().is_some_and(|txn| txn == txn_num) {
                buffer.flush();
            }
        }
    }

    fn waiting_too_long(&self, start_time: u128) -> bool {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() - start_time > Self::MAX_TIME
    }

    fn try_pin(&mut self, block_id: &BlockId) -> Option<&mut Buffer> {
        if let Some(buffer) = self.find_buffer(block_id) {
            Some(buffer)
        } else {
            self.find_unpinned_buffer()
        }
    }

    fn find_buffer(&mut self, block_id: &BlockId) -> Option<&mut Buffer> {
        for buffer in self.buffer_pool.iter_mut() {
            let buff_block_id = buffer.block_id();
            if let Some(blid) = buff_block_id {
                if blid == block_id {
                    return Some(buffer);
                }
            }
        }
        None
    }

    fn find_unpinned_buffer(&mut self) -> Option<&mut Buffer> {
        for buffer in self.buffer_pool.iter_mut() {
            if !buffer.pinned() {
                return Some(buffer);
            }
        }
        None
    }
}

#[cfg(test)]
mod buffer_tests {
    use tempdir::TempDir;
    use super::*;
    const TEST_BLOCK_SIZE: usize = 16;
    #[test]
    fn test_buffer() {
        let tmp_dir = TempDir::new("test_log_manager").expect("failed to create temp dir");
        let file_manager = Rc::new(RefCell::new(FileManager::new(
            tmp_dir.path().to_owned(),
            TEST_BLOCK_SIZE
        )));
        let mut log_manager = Rc::new(RefCell::new(
            LogManager::builder("log.wal".to_string(), file_manager.clone()).build(),
        ));

        let buffer = Buffer::new(file_manager.clone(), log_manager.clone());
        assert_eq!(buffer.pinned(), false);
        assert_eq!(buffer.txn, None);
        assert_eq!(buffer.lsn, None);
    }
}

#[cfg(test)]
mod buffer_manager_tests {}
