use byte_unit::Byte;

use std::time::Duration;

pub struct QueryResult {
    pub query_execution_id: String,
    pub data: Vec<Vec<String>>,
    pub data_scanned_bytes: i64,
    // query_execution_time_ms: i64,
    // query_planning_time_ms: i64,
    pub query_queue_time_ms: i64,
    pub rows: i64,
    pub columns: Vec<String>,
    pub total_execution_time_ms: i64
}

impl QueryResult {
    pub fn append_row(&mut self, row: Vec<String>) {
        self.data.push(row);
        self.rows += 1;
    }

    pub fn data_scanned(&self) -> String {
        let scanned = Byte::from_bytes(self.data_scanned_bytes as u128);
        let adjusted_byte = scanned.get_appropriate_unit(false);
        adjusted_byte.to_string()
    }

    pub fn total_time(&self) -> String {
        let time = Duration::from_millis(self.total_execution_time_ms as u64);
        humantime::format_duration(time).to_string()
    }
}