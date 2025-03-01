//  Copyright 2022. The Tari Project
//
//  Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
//  following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
//  disclaimer.
//
//  2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
//  following disclaimer in the documentation and/or other materials provided with the distribution.
//
//  3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
//  products derived from this software without specific prior written permission.
//
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
//  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
//  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
//  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
//  USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use tari_engine_types::indexed_value::IndexedValueError;
use tari_template_lib::{models::TemplateAddress, HashParseError};

use crate::{runtime::RuntimeError, template::TemplateLoaderError, wasm::WasmExecutionError};

#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error(transparent)]
    WasmExecutionError(#[from] WasmExecutionError),
    #[error("Template not found at address {address}")]
    TemplateNotFound { address: TemplateAddress },
    #[error(transparent)]
    RuntimeError(#[from] RuntimeError),
    #[error(transparent)]
    FlowEngineError(#[from] crate::flow::FlowEngineError),
    #[error("Failed to load template '{address}': {details}")]
    FailedToLoadTemplate { address: TemplateAddress, details: String },
    #[error("BOR error: {0}")]
    BorError(#[from] tari_bor::BorError),
    #[error("Value visitor error: {0}")]
    ValueVisitorError(#[from] IndexedValueError),
    #[error("Function {name} not found")]
    FunctionNotFound { name: String },
    #[error("Invariant error: {details}")]
    InvariantError { details: String },
    #[error("Load template error: {0}")]
    LoadTemplate(#[from] TemplateLoaderError),
    #[error("WASM binary too big! {0} bytes are greater than allowed maximum {1} bytes.")]
    WasmBinaryTooBig(usize, usize),
    #[error("Template provider error: {0}")]
    TemplateProvider(String),
    #[error("Converting to hash error: {0}")]
    HashConversion(#[from] HashParseError),
}
