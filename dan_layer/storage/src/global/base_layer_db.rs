//   Copyright 2022. The Tari Project
//
//   Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
//   following conditions are met:
//
//   1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
//   disclaimer.
//
//   2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
//   following disclaimer in the documentation and/or other materials provided with the distribution.
//
//   3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
//   products derived from this software without specific prior written permission.
//
//   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
//   INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//   DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
//   SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
//   WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
//   USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use std::fmt::Display;

use tari_common_types::types::FixedHash;
use tari_dan_common_types::Epoch;
use tari_sidechain::EvictionProof;

use crate::global::GlobalDbAdapter;

pub struct BaseLayerDb<'a, 'tx, TGlobalDbAdapter: GlobalDbAdapter> {
    backend: &'a TGlobalDbAdapter,
    tx: &'tx mut TGlobalDbAdapter::DbTransaction<'a>,
}

impl<'a, 'tx, TGlobalDbAdapter: GlobalDbAdapter> BaseLayerDb<'a, 'tx, TGlobalDbAdapter> {
    pub fn new(backend: &'a TGlobalDbAdapter, tx: &'tx mut TGlobalDbAdapter::DbTransaction<'a>) -> Self {
        Self { backend, tx }
    }

    pub fn insert_base_layer_block_info(&mut self, info: DbBaseLayerBlockInfo) -> Result<(), TGlobalDbAdapter::Error> {
        self.backend
            .insert_base_layer_block_info(self.tx, info)
            .map_err(TGlobalDbAdapter::Error::into)
    }

    pub fn get_base_layer_block_height(
        &mut self,
        hash: FixedHash,
    ) -> Result<Option<DbBaseLayerBlockInfo>, TGlobalDbAdapter::Error> {
        self.backend
            .get_base_layer_block_info(self.tx, hash)
            .map_err(TGlobalDbAdapter::Error::into)
    }

    pub fn insert_eviction_proof(&mut self, proof: &EvictionProof) -> Result<(), TGlobalDbAdapter::Error> {
        self.backend
            .insert_layer_one_transaction(self.tx, DbLayer1Transaction {
                epoch: Epoch(proof.epoch().as_u64()),
                proof_type: DbLayerOnePayloadType::EvictionProof,
                payload: proof,
            })
            .map_err(TGlobalDbAdapter::Error::into)
    }
}

#[derive(Debug, Clone)]
pub struct DbBaseLayerBlockInfo {
    pub hash: FixedHash,
    pub height: u64,
}

#[derive(Debug, Clone)]
pub struct DbLayer1Transaction<T> {
    pub epoch: Epoch,
    pub proof_type: DbLayerOnePayloadType,
    pub payload: T,
}

#[derive(Debug, Clone)]
pub enum DbLayerOnePayloadType {
    EvictionProof,
}

impl Display for DbLayerOnePayloadType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbLayerOnePayloadType::EvictionProof => write!(f, "EvictionProof"),
        }
    }
}
