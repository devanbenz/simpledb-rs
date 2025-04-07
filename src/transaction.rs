//public Transaction(FileMgr fm, LogMgr lm, BufferMgr bm);

use crate::filemanager::BlockId;

pub struct Transaction {}

impl Transaction {
    pub fn new() -> Self {}

    pub fn commit(&mut self) {}

    pub fn rollback(&mut self) {}

    pub fn recover(&mut self) {}

    pub fn pin(&mut self, block_id: &BlockId) {}

    pub fn unpin(&mut self, block_id: &BlockId) {}

    pub fn get_int(&self, offset: i32) -> Option<i32> {}

    pub fn get_string(&self,block_id: &BlockId, offset: usize) -> Option<String> {}

    pub fn set_int(&mut self, offset: i32, val: Option<i32>) {}

    pub fn set_string(&mut self, block_id: &BlockId, offset: i32, val: Option<String>, should_log: bool) {}

    pub fn available_buffers(&self) -> Option<usize> {}

    pub fn size() -> usize {}

    pub fn append() -> BlockId {}

    pub fn block_size() -> usize {}
}
