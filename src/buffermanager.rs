use crate::filemanager::{BlockId, FileManager, Page};
use crate::logmanager::LogManager;

struct Buffer {
    file_manager: FileManager,
    log_manager: LogManager,
}

impl Buffer {
    pub fn new() -> Buffer {}

    pub fn contents() -> Page {}

    pub fn block_id() -> BlockId {}

    pub fn pinned() -> bool {}

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
