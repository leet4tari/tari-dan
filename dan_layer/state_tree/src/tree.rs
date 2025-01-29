//   Copyright 2024 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::marker::PhantomData;

use serde::{Deserialize, Serialize};
use tari_common_types::types::FixedHash;
use tari_dan_common_types::VersionedSubstateId;
use tari_jellyfish::{
    JellyfishMerkleTree,
    LeafKey,
    Node,
    NodeKey,
    ProofValue,
    SparseMerkleProofExt,
    StaleTreeNode,
    TreeHash,
    TreeStore,
    TreeStoreReader,
    TreeUpdateBatch,
    Version,
};

use crate::{
    error::StateTreeError,
    key_mapper::{DbKeyMapper, HashIdentityKeyMapper, SpreadPrefixKeyMapper},
    memory_store::MemoryTreeStore,
    SPARSE_MERKLE_PLACEHOLDER_HASH,
};

pub type SpreadPrefixStateTree<'a, S> = StateTree<'a, S, SpreadPrefixKeyMapper>;
pub type RootStateTree<'a, S> = StateTree<'a, S, HashIdentityKeyMapper>;

pub struct StateTree<'a, S, M> {
    store: &'a mut S,
    _mapper: PhantomData<M>,
}

impl<'a, S, M> StateTree<'a, S, M> {
    pub fn new(store: &'a mut S) -> Self {
        Self {
            store,
            _mapper: PhantomData,
        }
    }
}

impl<S: TreeStoreReader<Version>, M: DbKeyMapper<VersionedSubstateId>> StateTree<'_, S, M> {
    pub fn get_proof(
        &self,
        version: Version,
        key: &VersionedSubstateId,
    ) -> Result<(LeafKey, Option<ProofValue<Version>>, SparseMerkleProofExt), StateTreeError> {
        let jmt = JellyfishMerkleTree::new(self.store);
        let key = M::map_to_leaf_key(key);
        let (maybe_value, proof) = jmt.get_with_proof_ext(key.as_ref(), version)?;
        Ok((key, maybe_value, proof))
    }

    pub fn get_root_hash(&self, version: Version) -> Result<TreeHash, StateTreeError> {
        let jmt = JellyfishMerkleTree::new(self.store);
        let root_hash = jmt.get_root_hash(version)?;
        Ok(root_hash)
    }
}

impl<S: TreeStore<Version>, M: DbKeyMapper<VersionedSubstateId>> StateTree<'_, S, M> {
    fn calculate_substate_changes<I: IntoIterator<Item = SubstateTreeChange>>(
        &mut self,
        current_version: Option<Version>,
        next_version: Version,
        changes: I,
    ) -> Result<(TreeHash, StateHashTreeDiff<Version>), StateTreeError> {
        let (root_hash, update_batch) =
            calculate_substate_changes::<_, M, _>(self.store, current_version, next_version, changes)?;
        Ok((root_hash, update_batch.into()))
    }

    /// Stores the substate changes in the state tree and returns the new root hash.
    pub fn put_substate_changes<I: IntoIterator<Item = SubstateTreeChange>>(
        &mut self,
        current_version: Option<Version>,
        next_version: Version,
        changes: I,
    ) -> Result<TreeHash, StateTreeError> {
        let (root_hash, update_batch) = self.calculate_substate_changes(current_version, next_version, changes)?;
        self.commit_diff(update_batch)?;
        Ok(root_hash)
    }

    pub fn commit_diff(&mut self, diff: StateHashTreeDiff<Version>) -> Result<(), StateTreeError> {
        for (key, node) in diff.new_nodes {
            log::debug!("Inserting node: {}", key);
            self.store.insert_node(key, node)?;
        }

        for stale_tree_node in diff.stale_tree_nodes {
            log::debug!("Recording stale tree node: {}", stale_tree_node.as_node_key());
            self.store.record_stale_tree_node(stale_tree_node)?;
        }

        Ok(())
    }
}

impl<S: TreeStore<()>, M: DbKeyMapper<TreeHash>> StateTree<'_, S, M> {
    pub fn put_changes<I: IntoIterator<Item = TreeHash>>(
        &mut self,
        current_version: Option<Version>,
        next_version: Version,
        changes: I,
    ) -> Result<TreeHash, StateTreeError> {
        let (root_hash, update_result) = self.compute_update_batch(current_version, next_version, changes)?;

        for (k, node) in update_result.node_batch {
            self.store.insert_node(k, node)?;
        }

        for stale_tree_node in update_result.stale_node_index_batch {
            self.store
                .record_stale_tree_node(StaleTreeNode::Node(stale_tree_node.node_key))?;
        }

        Ok(root_hash)
    }

    pub fn compute_update_batch<I: IntoIterator<Item = TreeHash>>(
        &mut self,
        current_version: Option<Version>,
        next_version: Version,
        changes: I,
    ) -> Result<(TreeHash, TreeUpdateBatch<()>), StateTreeError> {
        let jmt = JellyfishMerkleTree::<_, ()>::new(self.store);

        let changes = changes
            .into_iter()
            .map(|hash| (M::map_to_leaf_key(&hash), Some((hash, ()))));

        let (root, update) = jmt.batch_put_value_set(changes, None, current_version, next_version)?;
        Ok((root, update))
    }
}

/// Calculates the new root hash and tree updates for the given substate changes.
fn calculate_substate_changes<
    S: TreeStoreReader<Version>,
    M: DbKeyMapper<VersionedSubstateId>,
    I: IntoIterator<Item = SubstateTreeChange>,
>(
    store: &mut S,
    current_version: Option<Version>,
    next_version: Version,
    changes: I,
) -> Result<(TreeHash, TreeUpdateBatch<Version>), StateTreeError> {
    let jmt = JellyfishMerkleTree::new(store);

    let changes = changes.into_iter().map(|ch| match ch {
        SubstateTreeChange::Up { id, value_hash } => (
            M::map_to_leaf_key(&id),
            Some((TreeHash::new(value_hash.into_array()), next_version)),
        ),
        SubstateTreeChange::Down { id } => (M::map_to_leaf_key(&id), None),
    });

    let (root_hash, update_result) = jmt.batch_put_value_set(changes, None, current_version, next_version)?;

    Ok((root_hash, update_result))
}

pub enum SubstateTreeChange {
    Up {
        id: VersionedSubstateId,
        value_hash: FixedHash,
    },
    Down {
        id: VersionedSubstateId,
    },
}

impl SubstateTreeChange {
    pub fn id(&self) -> &VersionedSubstateId {
        match self {
            Self::Up { id, .. } => id,
            Self::Down { id } => id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StateHashTreeDiff<P> {
    pub new_nodes: Vec<(NodeKey, Node<P>)>,
    pub stale_tree_nodes: Vec<StaleTreeNode>,
}

impl<P> StateHashTreeDiff<P> {
    pub fn new() -> Self {
        Self {
            new_nodes: Vec::new(),
            stale_tree_nodes: Vec::new(),
        }
    }
}

impl<P> From<TreeUpdateBatch<P>> for StateHashTreeDiff<P> {
    fn from(batch: TreeUpdateBatch<P>) -> Self {
        Self {
            new_nodes: batch.node_batch,
            stale_tree_nodes: batch
                .stale_node_index_batch
                .into_iter()
                .map(|node| StaleTreeNode::Node(node.node_key))
                .collect(),
        }
    }
}

pub fn compute_merkle_root_for_hashes<I: Iterator<Item = TreeHash>>(hashes: I) -> Result<TreeHash, StateTreeError> {
    let mut hashes = hashes.peekable();
    if hashes.peek().is_none() {
        return Ok(SPARSE_MERKLE_PLACEHOLDER_HASH);
    }
    let mut mem_store = MemoryTreeStore::new();
    let mut root_tree = RootStateTree::new(&mut mem_store);
    let (hash, _) = root_tree.compute_update_batch(None, 1, hashes)?;
    Ok(hash)
}

/// Computes a Merkle proof for the given hash is either included in the provided the hashes, or proof of absence.
/// Returns the value (if it exists) and the Merkle proof.
pub fn compute_proof_for_hashes<I: Iterator<Item = TreeHash>>(
    hashes: I,
    hash_to_prove: TreeHash,
) -> Result<(Option<ProofValue<()>>, SparseMerkleProofExt), StateTreeError> {
    let mut mem_store = MemoryTreeStore::new();
    let mut root_tree = RootStateTree::new(&mut mem_store);
    root_tree.put_changes(None, 1, hashes)?;
    let jmt = JellyfishMerkleTree::new(&mem_store);
    let key = HashIdentityKeyMapper::map_to_leaf_key(&hash_to_prove);
    let proof_tuple = jmt.get_with_proof_ext(key.as_ref(), 1)?;
    Ok(proof_tuple)
}
