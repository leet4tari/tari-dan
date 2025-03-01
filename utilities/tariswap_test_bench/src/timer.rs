//   Copyright 2025 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::time::Instant;

use log::log;

pub struct TraceTimer {
    start: Instant,
    log_target: &'static str,
    context: &'static str,
    level: log::Level,
    iterations: Option<usize>,
}

impl TraceTimer {
    pub fn new(log_target: &'static str, context: &'static str, level: log::Level) -> Self {
        Self {
            start: Instant::now(),
            log_target,
            context,
            level,
            iterations: None,
        }
    }

    pub fn with_iterations(mut self, iterations: usize) -> Self {
        self.iterations = Some(iterations);
        self
    }

    pub fn info(log_target: &'static str, context: &'static str) -> Self {
        Self::new(log_target, context, log::Level::Info)
    }
}

impl Drop for TraceTimer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        if let Some(iterations) = self.iterations {
            let avg = elapsed.as_millis() as f64 / iterations as f64;
            log!(target: self.log_target, self.level, "⏲️ {} took {:.2?} for {} iterations (avg: {:.0?}ms)", self.context, elapsed, iterations, avg);
        } else {
            log!(target: self.log_target, self.level, "⏲️ {} took {:.2?}", self.context, elapsed);
        }
    }
}
