//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{collections::HashMap, sync::Arc};

use futures::{stream::FuturesOrdered, StreamExt};
use log::*;
use tari_dan_common_types::{NodeAddressable, SubstateRequirement, VersionedSubstateId};
use tari_engine_types::{
    indexed_value::IndexedValueError,
    substate::{Substate, SubstateId},
};
use tari_epoch_manager::EpochManagerReader;
use tari_transaction::Transaction;
use tari_validator_node_rpc::client::{SubstateResult, ValidatorNodeClientFactory};
use tokio::task::JoinError;

use crate::{
    error::IndexerError,
    substate_cache::SubstateCache,
    substate_decoder::find_related_substates,
    substate_scanner::SubstateScanner,
};

const LOG_TARGET: &str = "tari::indexer::transaction_autofiller";

#[derive(Debug, thiserror::Error)]
pub enum TransactionAutofillerError {
    #[error("Could not decode the substate: {0}")]
    IndexedValueVisitorError(#[from] IndexedValueError),
    #[error("Indexer error: {0}")]
    IndexerError(#[from] IndexerError),
    #[error("Tokio join error: {0}")]
    JoinError(#[from] JoinError),
}

pub struct TransactionAutofiller<TEpochManager, TVnClient, TSubstateCache> {
    substate_scanner: Arc<SubstateScanner<TEpochManager, TVnClient, TSubstateCache>>,
}

impl<TEpochManager, TVnClient, TAddr, TSubstateCache> TransactionAutofiller<TEpochManager, TVnClient, TSubstateCache>
where
    TEpochManager: EpochManagerReader<Addr = TAddr> + 'static,
    TVnClient: ValidatorNodeClientFactory<TAddr> + 'static,
    TAddr: NodeAddressable + 'static,
    TSubstateCache: SubstateCache + 'static,
{
    pub fn new(substate_scanner: Arc<SubstateScanner<TEpochManager, TVnClient, TSubstateCache>>) -> Self {
        Self { substate_scanner }
    }

    pub async fn autofill_transaction(
        &self,
        original_transaction: Transaction,
        substate_requirements: Vec<SubstateRequirement>,
    ) -> Result<(Transaction, HashMap<SubstateId, Substate>), TransactionAutofillerError> {
        if substate_requirements.is_empty() {
            return Ok((original_transaction, HashMap::new()));
        }
        // we will include the inputs and outputs into the "involved_objects" field
        // note that the transaction hash will not change as the "involved_objects" is not part of the hash
        let mut autofilled_transaction = original_transaction;

        // scan the network in parallel to fetch all the substates for each required input
        let mut versioned_inputs = vec![];
        let mut found_substates = HashMap::new();
        let substate_scanner_ref = self.substate_scanner.clone();
        let transaction_ref = Arc::new(autofilled_transaction.clone());
        let mut tasks = FuturesOrdered::new();
        for requirement in &substate_requirements {
            tasks.push_back(get_substate_requirement(
                substate_scanner_ref.clone(),
                transaction_ref.clone(),
                requirement.clone(),
            ));
        }
        while let Some(result) = tasks.next().await {
            if let Some((address, substate)) = result? {
                let versioned_substate_id = VersionedSubstateId::new(address.clone(), substate.version());
                versioned_inputs.push(versioned_substate_id);
                found_substates.insert(address, substate);
            }
        }
        info!(target: LOG_TARGET, "✏️️ Found {} input substates", found_substates.len());
        autofilled_transaction.filled_inputs_mut().extend(versioned_inputs);

        // let mut found_this_round = 0;

        const MAX_RECURSION: usize = 2;

        for _i in 0..MAX_RECURSION {
            // add all substates related to the inputs
            // TODO: we are going to only check the first level of recursion, for composability we may want to do it
            // recursively (with a recursion limit)
            let mut autofilled_inputs = vec![];
            let related_addresses: Vec<Vec<SubstateId>> = found_substates
                .values()
                .map(find_related_substates)
                .collect::<Result<_, _>>()?;

            info!(target: LOG_TARGET, "✏️️️ Found {} related substates", related_addresses.len());
            // exclude related substates that have been already included as requirement by the client
            let related_addresses = related_addresses
                .into_iter()
                .flatten()
                .filter(|s| !substate_requirements.iter().any(|r| r.substate_id() == s));

            // we need to fetch (in parallel) the latest version of all the related substates
            let mut handles = HashMap::new();
            let substate_scanner_ref = self.substate_scanner.clone();
            for id in related_addresses {
                info!(target: LOG_TARGET, "✏️️️ Found {} related substates", id);
                let handle = tokio::spawn(get_substate(substate_scanner_ref.clone(), id.clone(), None));
                handles.insert(id, handle);
            }
            for (address, handle) in handles {
                let scan_res = handle.await??;

                if let SubstateResult::Up { substate, id, .. } = scan_res {
                    info!(
                        target: LOG_TARGET,
                        "✏️ Filling related substate {}:v{}",
                        id,
                        substate.version()
                    );
                    autofilled_inputs.push(VersionedSubstateId::new(id, substate.version()));
                    found_substates.insert(address, substate);
                //       found_this_round += 1;
                } else {
                    warn!(
                        target: LOG_TARGET,
                        "✏️️ The related substate {} is not in UP status, skipping", address
                    );
                }
            }

            autofilled_transaction.filled_inputs_mut().extend(autofilled_inputs);
            //   if found_this_round == 0 {
            //      break;
            // }
        }

        Ok((autofilled_transaction, found_substates))
    }
}

pub async fn get_substate_requirement<TEpochManager, TVnClient, TAddr, TSubstateCache>(
    substate_scanner: Arc<SubstateScanner<TEpochManager, TVnClient, TSubstateCache>>,
    transaction: Arc<Transaction>,
    req: SubstateRequirement,
) -> Result<Option<(SubstateId, Substate)>, IndexerError>
where
    TEpochManager: EpochManagerReader<Addr = TAddr>,
    TVnClient: ValidatorNodeClientFactory<TAddr>,
    TAddr: NodeAddressable,
    TSubstateCache: SubstateCache,
{
    if transaction
        .all_inputs_iter()
        .any(|s| s.version.is_some() && s.substate_id() == req.substate_id())
    {
        // Input for this substate has a specified version, so we do not autofill it
        return Ok(None);
    }

    let mut version = req.version().unwrap_or(0);
    loop {
        let scan_res = substate_scanner.get_substate(req.substate_id(), Some(version)).await?;

        match scan_res {
            SubstateResult::DoesNotExist => {
                warn!( target: LOG_TARGET, "🖋️ The substate for input requirement {}:v{} does not exist, skipping", req.substate_id() , version);
                return Ok(None);
            },
            SubstateResult::Up { substate, id, .. } => {
                info!(
                    target: LOG_TARGET,
                    "Filling input substate {}:v{}",
                    id,
                    substate.version()
                );

                return Ok(Some((id, substate)));
            },
            SubstateResult::Down { id, .. } => {
                warn!(target: LOG_TARGET, "🖋️ The substate for input requirement {id}v{version} is DOWN, continuing to search");
                // Down in this shard, try the next version
                version += 1;
                continue;
            },
        }
    }
}

pub(crate) async fn get_substate<TEpochManager, TVnClient, TAddr, TSubstateCache>(
    substate_scanner: Arc<SubstateScanner<TEpochManager, TVnClient, TSubstateCache>>,
    substate_id: SubstateId,
    version_hint: Option<u32>,
) -> Result<SubstateResult, IndexerError>
where
    TEpochManager: EpochManagerReader<Addr = TAddr>,
    TVnClient: ValidatorNodeClientFactory<TAddr>,
    TAddr: NodeAddressable,
    TSubstateCache: SubstateCache,
{
    substate_scanner.get_substate(&substate_id, version_hint).await
}
