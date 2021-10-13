use metrics::counter;
use vector_core::internal_event::InternalEvent;

#[derive(Debug)]
pub struct CompoundErrorEvents {
    pub count: usize,
}

impl InternalEvent for CompoundErrorEvents {
    fn emit_metrics(&self) {
        counter!("compound_error_events_total", 1);
    }
}
