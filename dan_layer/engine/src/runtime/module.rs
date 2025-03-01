//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use crate::runtime::StateTracker;

pub trait RuntimeModule: Send + Sync {
    fn on_initialize(&self, _track: &StateTracker) -> Result<(), RuntimeModuleError> {
        Ok(())
    }

    fn on_runtime_call(&self, _track: &StateTracker, _call: &'static str) -> Result<(), RuntimeModuleError> {
        Ok(())
    }

    fn on_before_finalize(&self, _track: &StateTracker) -> Result<(), RuntimeModuleError> {
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeModuleError {
    #[error("BOR error: {0}")]
    Bor(#[from] tari_bor::BorError),
}
