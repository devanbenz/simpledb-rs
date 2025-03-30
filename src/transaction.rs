//public Transaction(FileMgr fm, LogMgr lm, BufferMgr bm);

pub struct Transaction {

}

impl Transaction {
    pub fn new() -> Self {}

    pub fn commit(&mut self) {}

    pub fn rollback(&mut self) {}

    pub fn recover(&mut self) {}

    pub fn pin(&mut self) {}

    pub fn unpin(&mut self) {}

    // public int getInt(BlockId blk, int offset);
    // public String getString(BlockId blk, int offset);
    // public void setInt(BlockId blk, int offset, int val,
    // boolean okToLog);
    // public void setString(BlockId blk, int offset, String val,
    // public int availableBuffs();
    // public int size(String filename);
    // public Block append(String filename);
    // public int blockSize();

    pub fn get_int(&self, offset: usize) -> Option<i32> {}

    pub fn get_string(&self, offset: usize) -> Option<String> {}

    pub fn set_int(&mut self, offset: usize, val: Option<i32>) {}

    pub fn set_string(&mut self, offset: usize, val: Option<String>) {}
}