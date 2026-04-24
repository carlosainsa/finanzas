use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    received_signals: AtomicU64,
    rejected_signals: AtomicU64,
    submitted_orders: AtomicU64,
    clob_errors: AtomicU64,
}

impl Metrics {
    pub fn signal_received(&self) {
        self.received_signals.fetch_add(1, Ordering::Relaxed);
    }

    pub fn signal_rejected(&self) {
        self.rejected_signals.fetch_add(1, Ordering::Relaxed);
    }

    pub fn order_submitted(&self) {
        self.submitted_orders.fetch_add(1, Ordering::Relaxed);
    }

    pub fn clob_error(&self) {
        self.clob_errors.fetch_add(1, Ordering::Relaxed);
    }
}
