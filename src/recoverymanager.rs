use crate::buffermanager::{Buffer, BufferManager};
use crate::filemanager::Page;
use crate::logmanager::{LogIterator, LogManager};
use crate::transaction::Transaction;
use std::cell::{Ref, RefCell};
use std::rc::Rc;
use crate::logrecord::{CommitLogRecord, LogRecordFactory, SetIntLogRecord, SetStringLogRecord, CHECKPOINT, COMMIT, ROLLBACK, START};

pub struct RecoveryManager {
    log_manager: Rc<RefCell<LogManager>>,
    buffer_manager: Rc<RefCell<BufferManager>>,
    transaction: Transaction,
    transaction_n: i32,
}

impl RecoveryManager {
    pub fn new(
        tx: Transaction,
        tx_n: i32,
        log_manager: Rc<RefCell<LogManager>>,
        buffer_manager: Rc<RefCell<BufferManager>>,
    ) -> RecoveryManager {
        RecoveryManager {
            log_manager,
            buffer_manager,
            transaction: tx,
            transaction_n: tx_n,
        }
    }

    pub fn commit(&self) {
        self.buffer_manager
            .borrow_mut()
            .flush_all_buffers(self.transaction_n);
        let lsn = CommitLogRecord::write_to_log_record(self.log_manager.clone(), self.transaction_n);
        self.log_manager.clone().borrow_mut().flush();
    }

    pub fn rollback(&mut self) {
        self.do_rollback();
        self.buffer_manager.borrow_mut().flush_all_buffers(self.transaction_n);
        let lsn = CommitLogRecord::write_to_log_record(self.log_manager.clone(), self.transaction_n);
        self.log_manager.clone().borrow_mut().flush();
    }

    pub fn recover(&mut self) {
        self.do_recover();
        self.buffer_manager.borrow_mut().flush_all_buffers(self.transaction_n);
        let lsn = CommitLogRecord::write_to_log_record(self.log_manager.clone(), self.transaction_n);
        self.log_manager.clone().borrow_mut().flush();
    }

    pub fn set_int(&mut self, buf: Buffer, offset: i32, new_val: i32) -> i32 {
        let old_value = buf.contents().borrow_mut().get_int(offset as usize).expect("no old value");
        if let Some(blid) = buf.block_id() {
            SetIntLogRecord::write_to_log_record(self.log_manager.clone(), buf.modifying_txn().unwrap(), blid, offset, old_value)
        } else {
            panic!("no old value")
        }
    }

    pub fn set_string(&mut self, buf: Buffer, offset: i32, new_val: String) -> i32 {
        let old_value = buf.contents().borrow_mut().get_string(offset as usize).expect("no old value");
        if let Some(blid) = buf.block_id() {
            SetStringLogRecord::write_to_log_record(self.log_manager.clone(), buf.modifying_txn().unwrap(), blid, offset, old_value)
        } else {
            panic!("no old value")
        }
    }

    fn do_rollback(&mut self) {
        let mut lit = self.log_manager.borrow_mut().iterator();
        while let Some(b) = lit.next() {
            if let Some(rec) = LogRecordFactory::create_log_record(b.to_vec()) {
                if rec.operation() == START {
                    return;
                } else {
                    rec.undo(&mut self.transaction)
                }
            }
        }
    }

    fn do_recover(&mut self) {
        let mut finished_txns = Vec::new();
        let mut lit = self.log_manager.borrow_mut().iterator();
        while let Some(b) = lit.next() {
            if let Some(rec) = LogRecordFactory::create_log_record(b.to_vec()) {
                if rec.operation() == CHECKPOINT { return; }
                if rec.operation() == COMMIT || rec.operation() == ROLLBACK {
                    finished_txns.push(rec);
                } else if !finished_txns.contains(&rec) {
                    rec.undo(&mut self.transaction);
                }
            }
        }
    }
}
