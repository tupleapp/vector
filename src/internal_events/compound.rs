use metrics::counter;
use vector_core::internal_event::InternalEvent;

#[derive(Debug)]
pub struct CompoundErrorEvents {
    pub count: usize,
}

impl InternalEvent for CompoundErrorEvents {
    fn emit_metrics(&self) {
        counter!("compound_error_events_total", self.count as u64);
    }
}

#[derive(Debug)]
pub struct CompoundTypeMismatchEventDropped {}

impl InternalEvent for CompoundTypeMismatchEventDropped {
    fn emit_metrics(&self) {
        counter!("compound_type_mismatch_events_dropped_total", 1);
    }
}
