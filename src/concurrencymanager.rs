use std::collections::HashMap;
use std::sync::Mutex;
use crate::filemanager::BlockId;

pub struct ConcurrencyManager {
    lock_table: Mutex<HashMap<BlockId, i32>>
}

impl ConcurrencyManager {
    pub fn new() -> ConcurrencyManager {
        let lock_table = Mutex::new(HashMap::new());
        ConcurrencyManager { lock_table }
    }

    pub fn acquire_s_lock(&mut self, block_id: &BlockId) {

    }

    pub fn acquire_x_lock(&mut self, block_id: &BlockId) {}

    pub fn release(&mut self) {}
}