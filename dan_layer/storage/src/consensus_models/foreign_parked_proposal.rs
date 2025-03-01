//    Copyright 2024 The Tari Project
//    SPDX-License-Identifier: BSD-3-Clause

use std::{fmt::Display, ops::Deref};

use tari_transaction::TransactionId;

use crate::{
    consensus_models::{Block, BlockPledge, ForeignProposal, QuorumCertificate},
    StateStoreReadTransaction,
    StateStoreWriteTransaction,
    StorageError,
};

#[derive(Debug, Clone)]
pub struct ForeignParkedProposal {
    proposal: ForeignProposal,
}

impl ForeignParkedProposal {
    pub fn new(proposal: ForeignProposal) -> Self {
        Self { proposal }
    }

    pub fn into_proposal(self) -> ForeignProposal {
        self.proposal
    }

    pub fn block(&self) -> &Block {
        &self.proposal.block
    }

    pub fn block_pledge(&self) -> &BlockPledge {
        &self.proposal.block_pledge
    }

    pub fn justify_qc(&self) -> &QuorumCertificate {
        &self.proposal.justify_qc
    }
}

impl ForeignParkedProposal {
    pub fn save<TTx>(&self, tx: &mut TTx) -> Result<bool, StorageError>
    where
        TTx: StateStoreWriteTransaction + Deref,
        TTx::Target: StateStoreReadTransaction,
    {
        if self.exists(&**tx)? {
            return Ok(false);
        }

        self.insert(tx)?;
        Ok(true)
    }

    pub fn insert<TTx: StateStoreWriteTransaction>(&self, tx: &mut TTx) -> Result<(), StorageError> {
        tx.foreign_parked_blocks_insert(self)
    }

    pub fn exists<TTx: StateStoreReadTransaction>(&self, tx: &TTx) -> Result<bool, StorageError> {
        tx.foreign_parked_blocks_exists(self.block().id())
    }

    pub fn add_missing_transactions<'a, TTx: StateStoreWriteTransaction, I: IntoIterator<Item = &'a TransactionId>>(
        &self,
        tx: &mut TTx,
        transaction_ids: I,
    ) -> Result<(), StorageError> {
        tx.foreign_parked_blocks_insert_missing_transactions(self.block().id(), transaction_ids)
    }

    pub fn remove_by_transaction_id<TTx: StateStoreWriteTransaction>(
        tx: &mut TTx,
        transaction_id: &TransactionId,
    ) -> Result<Vec<Self>, StorageError> {
        tx.foreign_parked_blocks_remove_all_by_transaction(transaction_id)
    }
}

impl Display for ForeignParkedProposal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ForeignParkedBlock: block={}, block_pledge=[{}], justify_qc={}",
            self.block(),
            self.block_pledge(),
            self.justify_qc()
        )
    }
}
