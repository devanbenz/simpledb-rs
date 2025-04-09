//public Transaction(FileMgr fm, LogMgr lm, BufferMgr bm);

use std::cell::{Ref, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use crate::buffermanager::{Buffer, BufferManager};
use crate::filemanager::{BlockId, FileManager};
use crate::recoverymanager::RecoveryManager;

struct BufferList<'a> {
    buffers: HashMap<&'a BlockId, Rc<RefCell<Buffer>>>,
    pins: Vec<&'a BlockId>,
    buffer_manager: Rc<RefCell<BufferManager>>,
}

impl BufferList {
    pub fn new(buffer_manager: Rc<RefCell<BufferManager>>) -> BufferList {
        BufferList {
            buffer_manager,
            pins: Vec::new(),
            buffers: HashMap::new(),
        }
    }

    pub fn get_buffer(&self, block_id: &BlockId) -> Option<&Rc<RefCell<Buffer>>> {
        self.buffers.get(block_id)
    }

    pub fn pin(&mut self, block_id: &BlockId) {
       if let Some(buffer) = self.buffer_manager.borrow_mut().pin(block_id) {
               self.buffers.insert(block_id, buffer);
               self.pins.push(block_id);
       }
    }

    pub fn unpin(&mut self, block_id: &BlockId) {
        if let Some(mut buffer) = self.buffers.get(block_id) {
            let buf_mut = buffer.get_mut();
            self.buffer_manager.borrow_mut().unpin(buf_mut);
            for (idx, val) in self.pins.iter().enumerate() {
                if *val == block_id {
                    self.pins.remove(idx);
                    break;
                }
            }
            if !self.pins.contains(&block_id) {
                self.buffers.remove(&block_id);
            }
        }
    }

    pub fn unpin_all(&mut self) {
        self.pins.clear();
        self.buffers.clear();
    }
}

pub struct Transaction<'a> {
    recovery_manager: Rc<RefCell<RecoveryManager>>,
    buffer_manager: Rc<RefCell<BufferManager>>,
    file_manager: Rc<RefCell<FileManager>>,
    buffer_list: BufferList<'a>,
    transaction_n: i32,
}

impl Transaction {
    pub fn new() -> Self {}

    pub fn commit(&mut self) {}

    pub fn rollback(&mut self) {}

    pub fn recover(&mut self) {}

    pub fn pin(&mut self, block_id: &BlockId) {}

    pub fn unpin(&mut self, block_id: &BlockId) {}

    pub fn get_int(&self, offset: i32) -> Option<i32> {}

    pub fn get_string(&self,block_id: &BlockId, offset: usize) -> Option<String> {}

    pub fn set_int(&mut self, block_id: &BlockId, offset: i32, val: Option<i32>, should_log: bool) {}

    pub fn set_string(&mut self, block_id: &BlockId, offset: i32, val: Option<String>, should_log: bool) {}

    pub fn available_buffers(&self) -> Option<usize> {}

    pub fn size() -> usize {}

    pub fn append() -> BlockId {}

    pub fn block_size() -> usize {}
}
