use crate::filemanager::{BlockId, Page};
use crate::logmanager::LogManager;
use crate::transaction::Transaction;
use std::cell::RefCell;
use std::rc::Rc;

pub const CHECKPOINT: i32 = 0;
pub const START: i32 = 1;
pub const COMMIT: i32 = 2;
pub const ROLLBACK: i32 = 3;
pub const SETINT: i32 = 4;
pub const SETSTRING: i32 = 5;

pub trait LogRecord {


    fn operation(&self) -> i32;

    fn tx_number(&self) -> i32;

    fn undo(&self, txn: &mut Transaction);

    fn create_log_record(bytes: Vec<u8>) -> Option<Box<dyn LogRecord>> {
        let mut page = Page::builder().with_log_buffer(bytes).build();
        let page_t = page.get_int(0).unwrap();
        match page_t {
            CHECKPOINT => Some(Box::new(SetIntLogRecord::new(page))),
            START => Some(Box::new(SetIntLogRecord::new(page))),
            COMMIT => Some(Box::new(SetIntLogRecord::new(page))),
            ROLLBACK => Some(Box::new(SetIntLogRecord::new(page))),
            SETINT => Some(Box::new(SetIntLogRecord::new(page))),
            SETSTRING => Some(Box::new(SetStringLogRecord::new(page))),
            _ => None,
        }
    }
}

pub struct LogRecordFactory;

impl LogRecordFactory {
    pub fn create_log_record(bytes: Vec<u8>) -> Option<Box<dyn LogRecord>> {
        let mut page = Page::builder().with_log_buffer(bytes).build();
        let page_t = page.get_int(0).unwrap();
        match page_t {
            CHECKPOINT => Some(Box::new(SetIntLogRecord::new(page))),
            START => Some(Box::new(SetIntLogRecord::new(page))),
            COMMIT => Some(Box::new(SetIntLogRecord::new(page))),
            ROLLBACK => Some(Box::new(SetIntLogRecord::new(page))),
            SETINT => Some(Box::new(SetIntLogRecord::new(page))),
            SETSTRING => Some(Box::new(SetStringLogRecord::new(page))),
            _ => None,
        }
    }
}

pub struct SetStringLogRecord {
    tx_number: i32,
    offset: i32,
    block_id: BlockId,
    value: String,
}

impl SetStringLogRecord {
    pub fn new(page: Page) -> SetStringLogRecord {
        let tx_pos = size_of::<i32>();
        let tx_number = page.get_int(tx_pos).unwrap();
        let filename_pos = tx_pos + size_of::<i32>();
        let filename = page.get_string(filename_pos).unwrap();
        let block_pos = Page::max_len(&filename);
        let block_num = page.get_int(block_pos).unwrap();
        let block_id = BlockId::new(&filename, block_num as usize);
        let offset_pos = block_pos + size_of::<i32>();
        let offset = page.get_int(offset_pos).unwrap();
        let value_pos = offset_pos + size_of::<i32>();
        let value = page.get_string(value_pos).unwrap();

        SetStringLogRecord {
            tx_number,
            offset,
            block_id,
            value,
        }
    }

    pub fn write_to_log_record(
        log_manager: Rc<RefCell<LogManager>>,
        tx_number: i32,
        block_id: &BlockId,
        offset: i32,
        value: String,
    ) -> i32 {
        let tx_pos = size_of::<i32>();
        let filename_pos = tx_pos + size_of::<i32>();
        let block_pos = Page::max_len(block_id.file_name().as_str());
        let offset_pos = block_pos + size_of::<i32>();
        let value_pos = offset_pos + size_of::<i32>();
        let record_len = value_pos + Page::max_len(&value);
        let record = vec![0u8; record_len];
        let mut page = Page::builder().with_log_buffer(record).build();
        page.set_int(0, Some(SETSTRING));
        page.set_int(tx_pos, Some(tx_number));
        page.set_string(filename_pos, Some(block_id.file_name()));
        page.set_int(block_pos, Some(block_id.block_num() as i32));
        page.set_int(offset_pos, Some(offset));
        page.set_string(value_pos, Some(value));
        let bb = page.bytes();
        log_manager.borrow_mut().append(Vec::from(bb))
    }
}
impl LogRecord for SetStringLogRecord {
    fn operation(&self) -> i32 {
        SETSTRING
    }

    fn tx_number(&self) -> i32 {
        self.tx_number
    }

    fn undo(&self, txn: &mut Transaction) {
        txn.pin(&self.block_id);
        txn.set_string(&self.block_id, self.offset, Some(self.value.clone()), false);
        txn.unpin(&self.block_id);
    }
}

pub struct SetIntLogRecord {
    tx_number: i32,
    offset: i32,
    block_id: BlockId,
    value: i32,
}

impl SetIntLogRecord {
    pub fn new(page: Page) -> SetIntLogRecord {
        let tx_pos = size_of::<i32>();
        let tx_number = page.get_int(tx_pos).unwrap();
        let filename_pos = tx_pos + size_of::<i32>();
        let filename = page.get_string(filename_pos).unwrap();
        let block_pos = Page::max_len(&filename);
        let block_num = page.get_int(block_pos).unwrap();
        let block_id = BlockId::new(&filename, block_num as usize);
        let offset_pos = block_pos + size_of::<i32>();
        let offset = page.get_int(offset_pos).unwrap();
        let value_pos = offset_pos + size_of::<i32>();
        let value = page.get_int(value_pos).unwrap();

        SetIntLogRecord {
            tx_number,
            offset,
            block_id,
            value,
        }
    }

    pub fn write_to_log_record(
        log_manager: Rc<RefCell<LogManager>>,
        tx_number: i32,
        block_id: &BlockId,
        offset: i32,
        value: i32,
    ) -> i32 {
        let tx_pos = size_of::<i32>();
        let filename_pos = tx_pos + size_of::<i32>();
        let block_pos = Page::max_len(block_id.file_name().as_str());
        let offset_pos = block_pos + size_of::<i32>();
        let value_pos = offset_pos + size_of::<i32>();
        let record_len = value_pos + size_of::<i32>();
        let record = vec![0u8; record_len];
        let mut page = Page::builder().with_log_buffer(record).build();
        page.set_int(0, Some(SETINT));
        page.set_int(tx_pos, Some(tx_number));
        page.set_string(filename_pos, Some(block_id.file_name()));
        page.set_int(block_pos, Some(block_id.block_num() as i32));
        page.set_int(offset_pos, Some(offset));
        page.set_int(value_pos, Some(value));
        let bb = page.bytes();
        log_manager.borrow_mut().append(Vec::from(bb))
    }
}
impl LogRecord for SetIntLogRecord {
    fn operation(&self) -> i32 {
        SETINT
    }

    fn tx_number(&self) -> i32 {
        self.tx_number
    }

    fn undo(&self, txn: &mut Transaction) {
        txn.pin(&self.block_id);
        txn.set_int(&self.block_id, self.offset, Some(self.value.clone()), false);
        txn.unpin(&self.block_id);
    }
}

pub struct CommitLogRecord {
    tx_number: i32,
}

impl CommitLogRecord {
    pub fn new(page: Page) -> CommitLogRecord {
        let tx_pos = size_of::<i32>();
        let tx_number = page.get_int(tx_pos).unwrap();

        CommitLogRecord {
            tx_number,
        }
    }

    pub fn write_to_log_record(
        log_manager: Rc<RefCell<LogManager>>,
        tx_number: i32,
    ) -> i32 {
        let tx_pos = size_of::<i32>();
        let record_len = tx_pos + size_of::<i32>();
        let record = vec![0u8; record_len];
        let mut page = Page::builder().with_log_buffer(record).build();
        page.set_int(0, Some(COMMIT));
        page.set_int(tx_pos, Some(tx_number));
        let bb = page.bytes();
        log_manager.borrow_mut().append(Vec::from(bb))
    }
}
impl LogRecord for CommitLogRecord {
    fn operation(&self) -> i32 {
        COMMIT
    }

    fn tx_number(&self) -> i32 {
        self.tx_number
    }

    fn undo(&self, txn: &mut Transaction) {
        txn.rollback();
    }
}

