//  Copyright 2023, The Tari Project
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

use log::*;
use rand::{prelude::*, rngs::OsRng};
use tari_dan_common_types::{displayable::Displayable, NodeAddressable, SubstateRequirement};
use tari_dan_storage::consensus_models::BlockId;
use tari_engine_types::{
    events::Event,
    substate::{SubstateId, SubstateValue},
};
use tari_epoch_manager::EpochManagerReader;
use tari_template_lib::{models::NonFungibleIndexAddress, prelude::ResourceAddress};
use tari_transaction::TransactionId;
use tari_validator_node_rpc::client::{SubstateResult, ValidatorNodeClientFactory, ValidatorNodeRpcClient};

use crate::{
    error::IndexerError,
    substate_cache::{SubstateCache, SubstateCacheEntry},
    NonFungibleSubstate,
};

const LOG_TARGET: &str = "tari::indexer::dan_layer_scanner";

#[derive(Debug, Clone)]
pub struct SubstateScanner<TEpochManager, TVnClient, TSubstateCache> {
    committee_provider: TEpochManager,
    validator_node_client_factory: TVnClient,
    substate_cache: TSubstateCache,
}

impl<TEpochManager, TVnClient, TAddr, TSubstateCache> SubstateScanner<TEpochManager, TVnClient, TSubstateCache>
where
    TAddr: NodeAddressable,
    TEpochManager: EpochManagerReader<Addr = TAddr>,
    TVnClient: ValidatorNodeClientFactory<TAddr>,
    TSubstateCache: SubstateCache,
{
    pub fn new(
        committee_provider: TEpochManager,
        validator_node_client_factory: TVnClient,
        substate_cache: TSubstateCache,
    ) -> Self {
        Self {
            committee_provider,
            validator_node_client_factory,
            substate_cache,
        }
    }

    pub async fn get_non_fungibles(
        &self,
        resource_address: &ResourceAddress,
        start_index: u64,
        end_index: Option<u64>,
    ) -> Result<Vec<NonFungibleSubstate>, IndexerError> {
        let mut nft_substates = vec![];
        let mut index = start_index;

        loop {
            // build the address of the nft index substate
            let index_address = NonFungibleIndexAddress::new(*resource_address, index);
            let index_substate_address = SubstateId::NonFungibleIndex(index_address);

            // get the nft index substate from the network
            // nft index substates are immutable, so they are always on version 0
            let index_substate_result = self
                .get_specific_substate_from_committee(index_substate_address, 0)
                .await?;
            let index_substate = match index_substate_result {
                SubstateResult::Up { substate, .. } => substate.into_substate_value(),
                _ => break,
            };

            // now that we have the index substate, we need the latest substate of the referenced nft
            let nft_address = match index_substate.into_non_fungible_index() {
                Some(idx) => idx.referenced_address().clone(),
                // the protocol should never produce this scenario, we stop querying for more indexes if it happens
                None => break,
            };
            let nft_id = SubstateId::NonFungible(nft_address);
            let SubstateResult::Up { substate, .. } = self.get_latest_substate_from_committee(&nft_id, None).await?
            else {
                break;
            };

            nft_substates.push(NonFungibleSubstate {
                index,
                address: nft_id,
                substate,
            });

            if let Some(end_index) = end_index {
                if index >= end_index {
                    break;
                }
            }

            index += 1;
        }

        Ok(nft_substates)
    }

    /// Attempts to find the latest substate for the given address. If the lowest possible version is known, it can be
    /// provided to reduce effort/time required to scan.
    pub async fn get_substate(
        &self,
        substate_id: &SubstateId,
        specific_version: Option<u32>,
    ) -> Result<SubstateResult, IndexerError> {
        debug!(target: LOG_TARGET, "get_substate: {}v{}", substate_id, specific_version.display());
        self.get_latest_substate_from_committee(substate_id, specific_version)
            .await
    }

    async fn get_latest_substate_from_committee(
        &self,
        substate_id: &SubstateId,
        specific_version: Option<u32>,
    ) -> Result<SubstateResult, IndexerError> {
        let mut cached_version = None;
        // start from the latest cached version of the substate (if cached previously)
        if let Some(version) = specific_version {
            let cache_res = self.substate_cache.read(substate_id.to_address_string()).await?;
            if let Some(entry) = cache_res {
                if entry.version == version {
                    debug!(target: LOG_TARGET, "Substate cache hit for {} with version {}", entry.version, substate_id);
                    return Ok(entry.substate_result);
                }
                cached_version = Some(entry.version);
            }
        }

        let requirement = SubstateRequirement::new(substate_id.clone(), specific_version);

        let substate_result = self.get_substate_from_committee_by_requirement(&requirement).await?;
        debug!(target: LOG_TARGET, "Substate result for {} with version {}: {:?}", substate_id.to_address_string(), specific_version.display(), substate_result);
        if let Some(version) = substate_result.version() {
            let should_update_cache = cached_version.map_or(true, |v| v < version);
            if should_update_cache {
                debug!(target: LOG_TARGET, "Updating cached substate {} with version {}", substate_id.to_address_string(), version);
                let entry = SubstateCacheEntry {
                    version,
                    substate_result: substate_result.clone(),
                };
                self.substate_cache
                    .write(substate_id.to_address_string(), &entry)
                    .await?;
            }
        }

        Ok(substate_result)
    }

    /// Returns a specific version. If this is not found an error is returned.
    pub async fn get_specific_substate_from_committee(
        &self,
        substate_id: SubstateId,
        version: u32,
    ) -> Result<SubstateResult, IndexerError> {
        let substate_req = SubstateRequirement::versioned(substate_id, version);
        debug!(target: LOG_TARGET, "get_specific_substate_from_committee: {substate_req}");
        self.get_substate_from_committee_by_requirement(&substate_req).await
    }

    /// Returns a specific version. If this is not found an error is returned.
    pub async fn get_substate_from_committee_by_requirement(
        &self,
        substate_req: &SubstateRequirement,
    ) -> Result<SubstateResult, IndexerError> {
        let epoch = self.committee_provider.current_epoch().await?;
        let mut committee = self
            .committee_provider
            .get_committee_for_substate(epoch, substate_req.to_substate_address_zero_version())
            .await?;

        committee.shuffle();

        let f = (committee.members.len() - 1) / 3;
        let mut num_nexist_substate_results = 0;
        let mut last_error = None;
        for vn_addr in committee.addresses() {
            // TODO: we cannot request data from ourselves via p2p rpc - so we should exclude ourselves from requests
            debug!(target: LOG_TARGET, "Getting substate {} from vn {}", substate_req, vn_addr);

            match self.get_substate_from_vn(vn_addr, substate_req).await {
                Ok(substate_result) => {
                    debug!(target: LOG_TARGET, "Got substate result for {} from vn {}: {:?}", substate_req, vn_addr, substate_result);
                    match substate_result {
                        SubstateResult::Up { .. } | SubstateResult::Down { .. } => return Ok(substate_result),
                        SubstateResult::DoesNotExist => {
                            if num_nexist_substate_results > f {
                                return Ok(substate_result);
                            }
                            num_nexist_substate_results += 1;
                        },
                    }
                },
                Err(e) => {
                    // We ignore a single VN error and keep querying the rest of the committee
                    warn!(
                        target: LOG_TARGET,
                        "Could not get substate {} from vn {}: {}", substate_req, vn_addr, e
                    );
                    last_error = Some(e);
                },
            }
        }

        warn!(
            target: LOG_TARGET,
            "Could not get substate for shard {} from any of the validator nodes", substate_req,
        );

        if let Some(e) = last_error {
            return Err(e);
        }
        Ok(SubstateResult::DoesNotExist)
    }

    /// Gets a substate directly from querying a VN
    async fn get_substate_from_vn(
        &self,
        vn_addr: &TAddr,
        substate_requirement: &SubstateRequirement,
    ) -> Result<SubstateResult, IndexerError> {
        // build a client with the VN
        let mut client = self.validator_node_client_factory.create_client(vn_addr);
        let result = client
            .get_substate(substate_requirement)
            .await
            .map_err(|e| IndexerError::ValidatorNodeClientError(e.to_string()))?;
        Ok(result)
    }

    /// Queries the network to obtain events emitted in a single transaction
    pub async fn get_events_for_transaction(&self, transaction_id: TransactionId) -> Result<Vec<Event>, IndexerError> {
        let substate_id = SubstateId::TransactionReceipt(transaction_id.into_array().into());
        let substate = self.get_specific_substate_from_committee(substate_id, 0).await?;
        let substate_value = if let SubstateResult::Up { substate, .. } = substate {
            substate.substate_value().clone()
        } else {
            return Err(IndexerError::InvalidSubstateState);
        };
        let events = if let SubstateValue::TransactionReceipt(tx_receipt) = substate_value {
            tx_receipt.events
        } else {
            return Err(IndexerError::InvalidSubstateValue);
        };

        Ok(events)
    }

    /// Queries the network to obtain a transaction hash from a given substate id and version
    async fn get_transaction_hash_from_substate_address(
        &self,
        substate_req: &SubstateRequirement,
    ) -> Result<TransactionId, IndexerError> {
        let epoch = self.committee_provider.current_epoch().await?;
        let mut committee = self
            .committee_provider
            .get_committee_for_substate(epoch, substate_req.to_substate_address_zero_version())
            .await?;

        committee.members.shuffle(&mut OsRng);

        let mut transaction_hash = None;
        for member in committee.addresses() {
            match self.get_substate_from_vn(member, substate_req).await {
                Ok(substate_result) => match substate_result {
                    SubstateResult::Up {
                        created_by_tx: tx_hash, ..
                    } |
                    SubstateResult::Down {
                        created_by_tx: tx_hash, ..
                    } => {
                        transaction_hash = Some(tx_hash);
                        break;
                    },
                    SubstateResult::DoesNotExist => {
                        warn!(
                            target: LOG_TARGET,
                            "validator node: {} does not have state for {}",
                            member,
                            substate_req,
                        );
                        continue;
                    },
                },
                Err(e) => {
                    warn!(
                        target: LOG_TARGET,
                        "Could not find substate result for {substate_req}, with error = {e}",
                    );
                    continue;
                },
            }
        }

        transaction_hash.ok_or_else(|| {
            IndexerError::NotFoundTransaction(
                substate_req.substate_id().clone(),
                substate_req.version().unwrap_or_default(),
            )
        })
    }

    /// Queries the network to obtain all the events associated with a substate and
    /// a specific version.
    pub async fn get_events_for_substate_and_version(
        &self,
        substate_req: &SubstateRequirement,
    ) -> Result<Vec<Event>, IndexerError> {
        let transaction_id = self.get_transaction_hash_from_substate_address(substate_req).await?;

        match self.get_events_for_transaction(transaction_id).await {
            Ok(tx_events) => {
                // we need to filter all transaction events, by those corresponding
                // to the current component address
                let component_tx_events = tx_events
                    .into_iter()
                    .filter(|e| e.substate_id() == Some(substate_req.substate_id()))
                    .collect();
                Ok(component_tx_events)
            },
            Err(e) => Err(e),
        }
    }

    /// Queries the network to obtain all the events associated with a component,
    /// starting at an optional version (if `None`, starts from `0`).
    pub async fn get_events_for_substate(
        &self,
        substate_id: &SubstateId,
        version: Option<u32>,
    ) -> Result<Vec<Event>, IndexerError> {
        let mut events = vec![];
        let mut version = version.unwrap_or_default();

        loop {
            let substate_req = SubstateRequirement::versioned(substate_id.clone(), version);
            match self.get_events_for_substate_and_version(&substate_req).await {
                Ok(component_tx_events) => events.extend(component_tx_events),
                Err(IndexerError::NotFoundTransaction(..)) => return Ok(events),
                Err(e) => return Err(e),
            }

            version += 1;
        }
    }

    pub async fn scan_events(
        &self,
        start_block: Option<BlockId>,
        topic: Option<String>,
        substate_id: Option<SubstateId>,
    ) -> Result<Vec<Event>, IndexerError> {
        warn!(
            target: LOG_TARGET,
            "scan_events: start_block={:?}, topic={:?}, substate_id={:?}",
            start_block,
            topic,
            substate_id
        );
        Ok(vec![])
    }
}
