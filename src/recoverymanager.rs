use crate::buffermanager::{Buffer, BufferManager};
use crate::filemanager::Page;
use crate::logmanager::LogManager;
use crate::transaction::Transaction;
use std::cell::{Ref, RefCell};
use std::rc::Rc;

struct RecoveryManager {
    log_manager: Rc<RefCell<LogManager>>,
    buffer_manager: Rc<RefCell<BufferManager>>,
    transaction: Transaction,
    transaction_n: i32
}

impl RecoveryManager {
    pub fn new(tx: Transaction, tx_n: i32, log_manager: Rc<RefCell<LogManager>>, buffer_manager: Rc<RefCell<BufferManager>>) -> RecoveryManager {
        RecoveryManager { log_manager, buffer_manager, transaction: tx, transaction_n: tx_n }
    }

    pub fn commit(&self) {
        self.buffer_manager.borrow_mut().flush_all_buffers(self.transaction_n);

    }

    pub fn rollback() {}

    pub fn recover() {}

    pub fn set_int(buf: Buffer, offset: usize, new_val: i32) -> usize {}

    pub fn set_string(buf: Buffer, offset: usize, new_val: String) -> usize {}
}
