use crate::buffermanager::Buffer;
use crate::filemanager::Page;
use crate::logmanager::LogManager;
use crate::transaction::Transaction;
use std::cell::RefCell;
use std::rc::Rc;

pub trait LogRecord {
    fn operation();

    fn tx_number();

    fn undo(tx_number: u32);

    fn create_log_record(bytes: Vec<u8>) -> Box<dyn LogRecord> {
        #[repr(i32)]
        enum LogRecordType {
            CHECKPOINT(i32) = 0,
            START(i32) = 1,
            COMMIT(i32) = 2,
            ROLLBACK(i32) = 3,
            SETINT(i32) = 4,
            SETSTRING(i32) = 5,
        }

        let mut page = Page::builder().with_log_buffer(bytes).build();
        let page_t = page.get_int(0).unwrap();
        match page_t {
            LogRecordType::CHECKPOINT(_) => {},
            START
        }
    }
}

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
