use crate::filemanager::{BlockId, FileManager, Page};
use crate::logmanager::LogManager;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicI32, Ordering};

struct Buffer {
    file_manager: Rc<RefCell<FileManager>>,
    log_manager: Rc<RefCell<LogManager>>,
    block_id: Option<BlockId>,
    contents: Page,
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

        let page = Page::builder()
            .block_size(fm_blk_size)
            .with_buffer()
            .build();
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

    pub fn contents(&self) -> &Page {
        let page = &self.contents;
        page
    }

    pub fn block_id(&self) -> &Option<BlockId> {
        &self.block_id
    }

    pub fn pinned(&self) -> bool {
        self.pins.load(Ordering::Relaxed) > 0
    }

    pub fn set_modified(txn: i32, lsn: i32) {}

    pub fn modifying_txn() -> i32 {}
}

// BufferMgr
// public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs);
struct BufferManager {
    file_manager: FileManager,
    log_manager: LogManager,
    buff_n: i32,
}

impl BufferManager {
    fn new() -> BufferManager {}
    pub fn pin(block_id: BlockId) -> Buffer {}

    pub fn unpin(buffer: Buffer) {}

    pub fn available_buffers() -> i32 {}

    pub fn flush_all_buffers(txn_num: i32) {}
}
