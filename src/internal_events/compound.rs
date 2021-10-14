use metrics::counter;
use vector_core::internal_event::InternalEvent;

#[derive(Debug)]
pub struct CompoundErrorEvents {
    pub count: usize,
}

impl InternalEvent for CompoundErrorEvents {
    fn emit_metrics(&self) {
        counter!("processing_errors_total", self.count as u64, "error_type" => "transform_failed");
    }
}

#[derive(Debug)]
pub struct CompoundTypeMismatchEventDropped {}

impl InternalEvent for CompoundTypeMismatchEventDropped {
    fn emit_metrics(&self) {
        counter!("events_discarded_total", 1);
    }
}
