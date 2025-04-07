use crate::buffermanager::Buffer;
use crate::filemanager::Page;
use crate::logmanager::LogManager;
use crate::transaction::Transaction;
use std::cell::RefCell;
use std::rc::Rc;

struct RecoveryManager {}

impl RecoveryManager {
    pub fn new(tx: Transaction, tx_n: i32, log_manager: Rc<RefCell<LogManager>>) -> Self {}

    pub fn commit() {}

    pub fn rollback() {}

    pub fn recover() {}

    pub fn set_int(buf: Buffer, offset: usize, new_val: i32) -> usize {}

    pub fn set_string(buf: Buffer, offset: usize, new_val: String) -> usize {}
}

// public void commit();
// public void rollback();
// public void recover();
// public int setInt(Buffer buff, int offset, int newval);
// public int setString(Buffer buff, int offset, String newval);
