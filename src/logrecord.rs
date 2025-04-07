use crate::filemanager::Page;

pub trait LogRecord {
    const CHECKPOINT: i32 = 0;
    const START: i32 = 1;
    const COMMIT: i32 = 2;
    const ROLLBACK: i32 = 3;
    const SETINT: i32 = 4;
    const SETSTRING: i32 = 5;

    fn operation() -> i32;

    fn tx_number(&self) -> usize;

    fn undo(&mut self, tx_number: u32);

    fn create_log_record(bytes: Vec<u8>) -> Option<Box<dyn LogRecord>> {
        let mut page = Page::builder().with_log_buffer(bytes).build();
        let page_t = page.get_int(0).unwrap();
        match page_t {
            CHECKPOINT => {
                todo!()
            }
            START => {
                todo!()
            }
            COMMIT => {
                todo!()
            }
            ROLLBACK => {
                todo!()
            }
            SETINT => {
                todo!()
            }
            SETSTRING => Some(Box::new()),
            _ => {
                return None;
            }
        }
    }
}

struct SetStringLogRecord {
    tx_number: usize,
    offset: usize,
}

impl SetStringLogRecord {
    pub fn new() -> SetStringLogRecord {}
}
impl LogRecord for SetStringLogRecord {
    fn operation() -> i32 {
        Self::SETSTRING
    }

    fn tx_number(&self) -> usize {
        self.tx_number
    }

    fn undo(&mut self, tx_number: u32) {
        todo!()
    }
}
