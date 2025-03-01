//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::collections::HashSet;

use log::*;
use tari_dan_common_types::{optional::Optional, Epoch};
use tari_dan_p2p::{
    proto,
    proto::rpc::{sync_blocks_response::SyncData, QuorumCertificates, SyncBlocksResponse},
};
use tari_dan_storage::{
    consensus_models::{Block, BlockId, QuorumCertificate, SubstateCreatedProof, SubstateUpdate, TransactionRecord},
    StateStore,
    StateStoreReadTransaction,
    StorageError,
};
use tari_rpc_framework::RpcStatus;
use tokio::sync::mpsc;

const LOG_TARGET: &str = "tari::dan::rpc::sync_task";

const BLOCK_BUFFER_SIZE: usize = 15;

struct BlockData {
    block: Block,
    qcs: Vec<QuorumCertificate>,
    substates: Vec<SubstateUpdate>,
    transactions: Vec<TransactionRecord>,
    transaction_receipts: Vec<SubstateCreatedProof>,
}
type BlockBuffer = Vec<BlockData>;

pub struct BlockSyncTask<TStateStore: StateStore> {
    store: TStateStore,
    start_block_id: BlockId,
    up_to_epoch: Option<Epoch>,
    sender: mpsc::Sender<Result<SyncBlocksResponse, RpcStatus>>,
}

impl<TStateStore: StateStore> BlockSyncTask<TStateStore> {
    pub fn new(
        store: TStateStore,
        start_block_id: BlockId,
        up_to_epoch: Option<Epoch>,
        sender: mpsc::Sender<Result<SyncBlocksResponse, RpcStatus>>,
    ) -> Self {
        Self {
            store,
            start_block_id,
            up_to_epoch,
            sender,
        }
    }

    pub async fn run(mut self, req: proto::rpc::SyncBlocksRequest) -> Result<(), ()> {
        let mut buffer = Vec::with_capacity(BLOCK_BUFFER_SIZE);
        let mut current_block_id = self.start_block_id;
        let mut counter = 0;
        loop {
            match self.fetch_next_batch(&mut buffer, &current_block_id, &req) {
                Ok(last_block) => {
                    current_block_id = last_block;
                },
                Err(err) => {
                    self.send(Err(RpcStatus::log_internal_error(LOG_TARGET)(err))).await?;
                    return Err(());
                },
            }

            let num_items = buffer.len();
            debug!(
                target: LOG_TARGET,
                "Sending {} blocks to peer. Current block id: {}",
                num_items,
                current_block_id,
            );

            counter += buffer.len();
            for data in buffer.drain(..) {
                self.send_block_data(&req, data).await?;
            }

            // If we didn't fill up the buffer, send the final blocks
            if num_items < buffer.capacity() {
                debug!( target: LOG_TARGET, "Sync to last commit complete. Streamed {} item(s)", counter);
                break;
            }
        }

        // match self.fetch_last_blocks(&mut buffer, &current_block_id).await {
        //     Ok(_) => (),
        //     Err(err) => {
        //         self.send(Err(RpcStatus::log_internal_error(LOG_TARGET)(err))).await?;
        //         return Err(());
        //     },
        // }

        debug!(
            target: LOG_TARGET,
            "Sending {} last blocks to peer.",
            buffer.len(),
        );

        for data in buffer.drain(..) {
            self.send_block_data(&req, data).await?;
        }

        Ok(())
    }

    fn fetch_next_batch(
        &self,
        buffer: &mut BlockBuffer,
        current_block_id: &BlockId,
        req: &proto::rpc::SyncBlocksRequest,
    ) -> Result<BlockId, StorageError> {
        self.store.with_read_tx(|tx| {
            let mut current_block_id = *current_block_id;
            let mut last_block_id = current_block_id;
            loop {
                let current_block = tx.blocks_get(&current_block_id)?;

                // Find the next block in the database
                let child = if current_block.is_epoch_end() {
                    // The current block is the last one in the epoch,
                    // so we need to find the first block in the next expoch
                    tx.blocks_get_genesis_for_epoch(current_block.epoch() + Epoch(1))
                        .optional()?
                } else {
                    // The current block is NOT the last one in the epoch,
                    // so we need to find a child block
                    let children = tx.blocks_get_all_by_parent(&current_block_id)?;
                    children.into_iter().find(|b| b.is_committed())
                };

                // If there is not a new block then we stop streaming
                let Some(child) = child else {
                    break;
                };

                // If we hit the max allowed epoch then we stop streaming
                if let Some(epoch) = self.up_to_epoch {
                    if child.epoch() > epoch {
                        break;
                    }
                }

                current_block_id = *child.id();
                if child.is_dummy() {
                    continue;
                }

                last_block_id = current_block_id;
                let certificates = req
                    .stream_qcs
                    .then(|| {
                        child
                            .commands()
                            .iter()
                            .filter_map(|cmd| cmd.transaction())
                            .flat_map(|transaction| transaction.evidence.qc_ids_iter())
                            .collect::<HashSet<_>>()
                    })
                    .map(|all_qcs| QuorumCertificate::get_all(tx, all_qcs))
                    .transpose()?
                    .unwrap_or_default();
                let substates_selection =
                    proto::rpc::StreamSubstateSelection::try_from(req.stream_substates).map_err(|e| {
                        StorageError::General {
                            details: format!("{} is not a valid StreamSubstateSelection: {}", req.stream_substates, e),
                        }
                    })?;

                let updates = matches!(substates_selection, proto::rpc::StreamSubstateSelection::All)
                    .then(|| child.get_substate_updates(tx))
                    .transpose()?
                    .unwrap_or_default();
                let transaction_receipts = matches!(
                    substates_selection,
                    proto::rpc::StreamSubstateSelection::TransactionReceiptsOnly
                )
                .then(|| child.get_transaction_receipts(tx))
                .transpose()?
                .unwrap_or_default();

                let transactions = req
                    .stream_transactions
                    .then(|| child.get_transactions(tx))
                    .transpose()?
                    .unwrap_or_default();

                buffer.push(BlockData {
                    block: child,
                    qcs: certificates,
                    substates: updates,
                    transactions,
                    transaction_receipts,
                });
                if buffer.len() == buffer.capacity() {
                    break;
                }
            }
            Ok::<_, StorageError>(last_block_id)
        })
    }

    // async fn fetch_last_blocks(
    //     &self,
    //     buffer: &mut BlockBuffer,
    //     current_block_id: &BlockId,
    // ) -> Result<(), StorageError> {
    //     // if let Some(up_to_epoch) = self.up_to_epoch {
    //     //     // Wait for the end of epoch block if the requested epoch has not yet completed
    //     //     // TODO: We should consider streaming blocks as they come in from consensus
    //     //     loop {
    //     //         let block = self.store.with_read_tx(|tx| LockedBlock::get(tx)?.get_block(tx))?;
    //     //         if block.is_epoch_end() && block.epoch() + Epoch(1) >= up_to_epoch {
    //     //             // If found the epoch end block, break.
    //     //             break;
    //     //         }
    //     //         tokio::time::sleep(Duration::from_secs(10)).await;
    //     //     }
    //     // }
    //     self.store.with_read_tx(|tx| {
    //         // TODO: if there are any transactions in the block the syncing node will reject the block
    //
    //         // If syncing to epoch, sync to the leaf block
    //         let up_to_block = if self.up_to_epoch.is_none() {
    //             let locked_block = LockedBlock::get(tx)?;
    //             *locked_block.block_id()
    //         } else {
    //             let leaf_block = LeafBlock::get(tx)?;
    //             *leaf_block.block_id()
    //         };
    //
    //         let blocks = Block::get_all_blocks_between(tx, current_block_id, &up_to_block, false)?;
    //         for block in blocks {
    //             debug!(
    //                 target: LOG_TARGET,
    //                 "Fetching last blocks. Current block: {} to target {}",
    //                 block,
    //                 current_block_id
    //             );
    //             let all_qcs = block
    //                 .commands()
    //                 .iter()
    //                 .filter(|cmd| cmd.transaction().is_some())
    //                 .flat_map(|cmd| cmd.evidence().qc_ids_iter())
    //                 .collect::<HashSet<_>>();
    //             let certificates = QuorumCertificate::get_all(tx, all_qcs)?;
    //
    //             // No substate updates can occur for blocks after the last commit
    //             buffer.push((block, certificates, vec![], vec![]));
    //         }
    //
    //         Ok::<_, StorageError>(())
    //     })
    // }

    async fn send(&mut self, result: Result<SyncBlocksResponse, RpcStatus>) -> Result<(), ()> {
        if self.sender.send(result).await.is_err() {
            debug!(
                target: LOG_TARGET,
                "Peer stream closed by client before completing. Aborting"
            );
            return Err(());
        }
        Ok(())
    }

    async fn send_block_data(&mut self, req: &proto::rpc::SyncBlocksRequest, data: BlockData) -> Result<(), ()> {
        let BlockData {
            block,
            qcs,
            substates: updates,
            transactions,
            transaction_receipts,
        } = data;
        self.send(Ok(SyncBlocksResponse {
            sync_data: Some(SyncData::Block((&block).into())),
        }))
        .await?;

        if req.stream_qcs {
            self.send(Ok(SyncBlocksResponse {
                sync_data: Some(SyncData::QuorumCertificates(QuorumCertificates {
                    quorum_certificates: qcs.iter().map(Into::into).collect(),
                })),
            }))
            .await?;
        }

        match proto::rpc::StreamSubstateSelection::try_from(req.stream_substates).map_err(|_| ())? {
            proto::rpc::StreamSubstateSelection::No => {},
            proto::rpc::StreamSubstateSelection::All => {
                match u32::try_from(updates.len()) {
                    Ok(count) => {
                        self.send(Ok(SyncBlocksResponse {
                            sync_data: Some(SyncData::SubstateCount(count)),
                        }))
                        .await?;
                    },
                    Err(_) => {
                        self.send(Err(RpcStatus::general("number of substates exceeds u32")))
                            .await?;
                        return Err(());
                    },
                }
                for update in updates {
                    self.send(Ok(SyncBlocksResponse {
                        sync_data: Some(SyncData::SubstateUpdate(update.into())),
                    }))
                    .await?;
                }
            },
            proto::rpc::StreamSubstateSelection::TransactionReceiptsOnly => {
                match u32::try_from(transaction_receipts.len()) {
                    Ok(count) => {
                        self.send(Ok(SyncBlocksResponse {
                            sync_data: Some(SyncData::TransactionReceiptCount(count)),
                        }))
                        .await?;
                    },
                    Err(_) => {
                        self.send(Err(RpcStatus::general("number of substates exceeds u32")))
                            .await?;
                        return Err(());
                    },
                }
                for receipt in transaction_receipts {
                    self.send(Ok(SyncBlocksResponse {
                        sync_data: Some(SyncData::TransactionReceipt(receipt.into())),
                    }))
                    .await?;
                }
            },
        }

        if req.stream_transactions {
            match u32::try_from(transactions.len()) {
                Ok(count) => {
                    self.send(Ok(SyncBlocksResponse {
                        sync_data: Some(SyncData::TransactionCount(count)),
                    }))
                    .await?;
                },
                Err(_) => {
                    self.send(Err(RpcStatus::general("number of substates exceeds u32")))
                        .await?;
                    return Err(());
                },
            }
            for transaction in transactions {
                self.send(Ok(SyncBlocksResponse {
                    sync_data: Some(SyncData::Transaction(transaction.transaction().into())),
                }))
                .await?;
            }
        }

        Ok(())
    }
}
