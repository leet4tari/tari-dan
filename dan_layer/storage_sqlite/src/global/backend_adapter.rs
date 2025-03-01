//  Copyright 2022. The Tari Project
//
//  Redistribution and use in source and binary forms, with or without modification, are permitted provided that
// the  following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice, this list of conditions and the
// following  disclaimer.
//
//  2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
//  following disclaimer in the documentation and/or other materials provided with the distribution.
//
//  3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
//  products derived from this software without specific prior written permission.
//
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
// WARRANTIES,  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
// PARTICULAR PURPOSE ARE  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
// DIRECT, INDIRECT, INCIDENTAL,  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY,  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
// OTHERWISE) ARISING IN ANY WAY OUT OF THE  USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.
use std::{
    collections::{HashMap, HashSet},
    convert::{TryFrom, TryInto},
    fmt::{Debug, Formatter},
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use diesel::{
    sql_query,
    sql_types::{BigInt, Bigint},
    BoolExpressionMethods,
    ExpressionMethods,
    JoinOnDsl,
    OptionalExtension,
    QueryDsl,
    RunQueryDsl,
    SqliteConnection,
};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use log::debug;
use serde::{de::DeserializeOwned, Serialize};
use tari_common_types::types::{FixedHash, PublicKey};
use tari_dan_common_types::{
    committee::Committee,
    hashing::ValidatorNodeBalancedMerkleTree,
    Epoch,
    NodeAddressable,
    ShardGroup,
    SubstateAddress,
};
use tari_dan_storage::{
    global::{
        models::ValidatorNode,
        DbBaseLayerBlockInfo,
        DbEpoch,
        DbLayer1Transaction,
        DbTemplate,
        DbTemplateUpdate,
        GlobalDbAdapter,
        MetadataKey,
        TemplateStatus,
    },
    AtomicDb,
};
use tari_engine_types::TemplateAddress;
use tari_utilities::{hex, ByteArray};

use super::{models, models::DbValidatorNode};
use crate::{
    error::SqliteStorageError,
    global::{
        models::{
            DbCommittee,
            MetadataModel,
            NewBaseLayerBlockInfo,
            NewEpoch,
            NewTemplateModel,
            TemplateModel,
            TemplateUpdateModel,
        },
        serialization::serialize_json,
    },
    SqliteTransaction,
};

const LOG_TARGET: &str = "tari::dan::storage_sqlite::global::backend_adapter";

define_sql_function! {
    #[sql_name = "COALESCE"]
    fn coalesce_bigint(x: diesel::sql_types::Nullable<Bigint>, y: BigInt) -> BigInt;
}
define_sql_function! {
    #[sql_name = "random"]
    fn sql_random() -> Integer;
}

pub struct SqliteGlobalDbAdapter<TAddr> {
    connection: Arc<Mutex<SqliteConnection>>,
    _addr: PhantomData<TAddr>,
}

impl<TAddr> SqliteGlobalDbAdapter<TAddr> {
    pub fn new(connection: SqliteConnection) -> Self {
        Self {
            connection: Arc::new(Mutex::new(connection)),
            _addr: PhantomData,
        }
    }

    fn exists(&self, tx: &mut SqliteTransaction<'_>, key: MetadataKey) -> Result<bool, SqliteStorageError> {
        use crate::global::schema::metadata;
        let result = metadata::table
            .filter(metadata::key_name.eq(key.as_key_bytes()))
            .count()
            .limit(1)
            .get_result::<i64>(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "exists::metadata".to_string(),
            })?;
        Ok(result > 0)
    }

    pub fn migrate(&self) -> Result<(), SqliteStorageError> {
        const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");
        self.connection
            .lock()
            .unwrap()
            .run_pending_migrations(MIGRATIONS)
            .map_err(|source| SqliteStorageError::MigrationError { source })?;
        Ok(())
    }
}

impl<TAddr> AtomicDb for SqliteGlobalDbAdapter<TAddr> {
    type DbTransaction<'a> = SqliteTransaction<'a>;
    type Error = SqliteStorageError;

    fn create_transaction(&self) -> Result<Self::DbTransaction<'_>, Self::Error> {
        let tx = SqliteTransaction::begin(self.connection.lock().unwrap())?;
        Ok(tx)
    }

    fn commit(&self, transaction: Self::DbTransaction<'_>) -> Result<(), Self::Error> {
        transaction.commit()
    }
}

impl<TAddr: NodeAddressable> GlobalDbAdapter for SqliteGlobalDbAdapter<TAddr> {
    type Addr = TAddr;

    fn set_metadata<T: Serialize>(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        key: MetadataKey,
        value: &T,
    ) -> Result<(), Self::Error> {
        use crate::global::schema::metadata;
        let value = serde_json::to_vec(value)?;
        match self.exists(tx, key) {
            Ok(true) => diesel::update(metadata::table)
                .filter(metadata::key_name.eq(key.as_key_bytes()))
                .set(metadata::value.eq(value))
                .execute(tx.connection())
                .map_err(|source| SqliteStorageError::DieselError {
                    source,
                    operation: "update::metadata".to_string(),
                })?,
            Ok(false) => diesel::insert_into(metadata::table)
                .values((metadata::key_name.eq(key.as_key_bytes()), metadata::value.eq(value)))
                .execute(tx.connection())
                .map_err(|source| SqliteStorageError::DieselError {
                    source,
                    operation: "insert::metadata".to_string(),
                })?,
            Err(e) => return Err(e),
        };

        Ok(())
    }

    fn get_metadata<T: DeserializeOwned>(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        key: &MetadataKey,
    ) -> Result<Option<T>, Self::Error> {
        use crate::global::schema::metadata;

        let row: Option<MetadataModel> = metadata::table
            .find(key.as_key_bytes())
            .first(tx.connection())
            .optional()
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "get::metadata_key".to_string(),
            })?;

        let v = row.map(|r| serde_json::from_slice(&r.value)).transpose()?;
        Ok(v)
    }

    fn get_template(&self, tx: &mut Self::DbTransaction<'_>, key: &[u8]) -> Result<Option<DbTemplate>, Self::Error> {
        use crate::global::schema::templates;
        let template: Option<TemplateModel> = templates::table
            .filter(templates::template_address.eq(key))
            .first(tx.connection())
            .optional()
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "get_template".to_string(),
            })?;

        match template {
            Some(t) => Ok(Some(DbTemplate {
                author_public_key: PublicKey::from_canonical_bytes(&t.author_public_key)
                    .map_err(|e| SqliteStorageError::MalformedDbData(format!("Failed to decode public key:{e}")))?,
                template_name: t.template_name,
                expected_hash: t.expected_hash.try_into()?,
                template_address: t.template_address.try_into()?,
                template_type: t.template_type.parse().expect("DB template type corrupted"),
                epoch: Epoch(t.epoch as u64),
                code: t.code,
                url: t.url,
                status: t.status.parse().expect("DB status corrupted"),
                added_at: t.added_at,
            })),
            None => Ok(None),
        }
    }

    fn get_templates(&self, tx: &mut Self::DbTransaction<'_>, limit: usize) -> Result<Vec<DbTemplate>, Self::Error> {
        use crate::global::schema::templates;

        let mut templates = templates::table
            .filter(templates::status.eq(TemplateStatus::Active.as_str()))
            .into_boxed();

        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        if limit > 0 {
            templates = templates.limit(limit);
        }
        let templates = templates
            .get_results::<TemplateModel>(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "get_templates".to_string(),
            })?;

        templates
            .into_iter()
            .map(|t| t.try_into().map_err(SqliteStorageError::TemplateConversion))
            .collect()
    }

    fn get_templates_by_addresses(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        addresses: Vec<&[u8]>,
    ) -> Result<Vec<DbTemplate>, Self::Error> {
        use crate::global::schema::templates;

        templates::table
            .filter(templates::status.eq(TemplateStatus::Active.as_str()))
            .filter(templates::template_address.eq_any(addresses))
            .get_results::<TemplateModel>(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "get_templates_by_addresses".to_string(),
            })?
            .into_iter()
            .map(|t| t.try_into().map_err(SqliteStorageError::TemplateConversion))
            .collect()
    }

    fn get_pending_templates(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        limit: usize,
    ) -> Result<Vec<DbTemplate>, Self::Error> {
        use crate::global::schema::templates;
        let templates = templates::table
            .filter(templates::status.eq(TemplateStatus::Pending.as_str()))
            .limit(i64::try_from(limit).unwrap_or(i64::MAX))
            .get_results::<TemplateModel>(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "get_pending_template".to_string(),
            })?;

        templates
            .into_iter()
            .map(|t| {
                Ok(DbTemplate {
                    author_public_key: PublicKey::from_canonical_bytes(&t.author_public_key).map_err(|e| {
                        SqliteStorageError::MalformedDbData(format!("Failed to decode public key: {e}"))
                    })?,
                    template_name: t.template_name,
                    expected_hash: t.expected_hash.try_into()?,
                    template_address: TemplateAddress::try_from_vec(t.template_address)?,
                    template_type: t.template_type.parse().expect("DB template type corrupted"),
                    code: t.code,
                    url: t.url,
                    status: t.status.parse().expect("DB status corrupted"),
                    added_at: t.added_at,
                    epoch: Epoch(t.epoch as u64),
                })
            })
            .collect()
    }

    fn insert_template(&self, tx: &mut Self::DbTransaction<'_>, item: DbTemplate) -> Result<(), Self::Error> {
        use crate::global::schema::templates;
        let new_template = NewTemplateModel {
            author_public_key: item.author_public_key.to_vec(),
            template_name: item.template_name,
            expected_hash: item.expected_hash.to_vec(),
            template_address: item.template_address.to_vec(),
            template_type: item.template_type.as_str().to_string(),
            code: item.code,
            epoch: item.epoch.as_u64() as i64,
            status: item.status.as_str().to_string(),
        };
        diesel::insert_into(templates::table)
            .values(new_template)
            .execute(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "insert_template".to_string(),
            })?;

        Ok(())
    }

    fn update_template(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        key: &[u8],
        template: DbTemplateUpdate,
    ) -> Result<(), Self::Error> {
        use crate::global::schema::templates;

        let model = TemplateUpdateModel {
            author_public_key: template.author_public_key.map(|pk| pk.to_vec()),
            expected_hash: template.expected_hash.map(|hash| hash.to_vec()),
            template_type: template.template_type.map(|tmpl_type| tmpl_type.as_str().to_string()),
            template_name: template.template_name,
            epoch: template.epoch.map(|epoch| epoch.as_u64() as i64),
            code: template.code.map(Some),
            status: template.status.map(|s| s.as_str().to_string()),
        };
        diesel::update(templates::table)
            .filter(templates::template_address.eq(key))
            .set(model)
            .execute(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "update_template".to_string(),
            })?;

        Ok(())
    }

    fn template_exists(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        key: &[u8],
        status: Option<TemplateStatus>,
    ) -> Result<bool, Self::Error> {
        use crate::global::schema::templates;

        let mut query = templates::table
            .filter(templates::template_address.eq(key))
            .into_boxed();
        if let Some(status) = status {
            query = query.filter(templates::status.eq(status.as_str()));
        }

        let result = query
            .count()
            .limit(1)
            .get_result::<i64>(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "exists::metadata".to_string(),
            })?;
        Ok(result > 0)
    }

    fn set_status(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        key: &TemplateAddress,
        status: TemplateStatus,
    ) -> Result<(), Self::Error> {
        use crate::global::schema::templates;
        let num_affected = diesel::update(templates::table)
            .filter(templates::template_address.eq(key.as_ref()))
            .set(templates::status.eq(status.as_str()))
            .execute(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "set_status".to_string(),
            })?;
        if num_affected == 0 {
            return Err(SqliteStorageError::NotFound {
                item: "template",
                key: hex::to_hex(key),
            });
        }
        Ok(())
    }

    fn insert_validator_node(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        address: Self::Addr,
        public_key: PublicKey,
        shard_key: SubstateAddress,
        start_epoch: Epoch,
        fee_claim_public_key: PublicKey,
    ) -> Result<(), Self::Error> {
        use crate::global::schema::validator_nodes;
        let addr = serialize_json(&address)?;

        diesel::insert_into(validator_nodes::table)
            .values((
                validator_nodes::address.eq(&addr),
                validator_nodes::public_key.eq(ByteArray::as_bytes(&public_key)),
                validator_nodes::shard_key.eq(shard_key.as_bytes()),
                validator_nodes::start_epoch.eq(start_epoch.as_u64() as i64),
                validator_nodes::fee_claim_public_key.eq(ByteArray::as_bytes(&fee_claim_public_key)),
            ))
            .execute(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "insert::validator_nodes".to_string(),
            })?;

        Ok(())
    }

    fn deactivate_validator_node(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        public_key: PublicKey,
        deactivation_epoch: Epoch,
    ) -> Result<(), Self::Error> {
        use crate::global::schema::validator_nodes;

        diesel::update(validator_nodes::table)
            .set(validator_nodes::end_epoch.eq(deactivation_epoch.as_u64() as i64))
            .filter(validator_nodes::public_key.eq(ByteArray::as_bytes(&public_key)))
            .execute(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "remove::validator_nodes".to_string(),
            })?;

        Ok(())
    }

    fn get_validator_node_by_address(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        epoch: Epoch,
        address: &Self::Addr,
    ) -> Result<ValidatorNode<Self::Addr>, Self::Error> {
        use crate::global::schema::{committees, validator_nodes};

        let vn = validator_nodes::table
            .select(validator_nodes::all_columns)
            .inner_join(committees::table.on(validator_nodes::id.eq(committees::validator_node_id)))
            .filter(committees::epoch.eq(epoch.as_u64() as i64))
            .filter(validator_nodes::address.eq(serialize_json(address)?))
            .order_by(validator_nodes::id.desc())
            .first::<DbValidatorNode>(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "get::validator_node".to_string(),
            })?;

        let vn = vn.try_into()?;
        Ok(vn)
    }

    fn get_validator_node_by_public_key(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        epoch: Epoch,
        public_key: &PublicKey,
    ) -> Result<ValidatorNode<Self::Addr>, Self::Error> {
        use crate::global::schema::{committees, validator_nodes};

        let vn = validator_nodes::table
            .select(validator_nodes::all_columns)
            .inner_join(committees::table.on(validator_nodes::id.eq(committees::validator_node_id)))
            .filter(committees::epoch.eq(epoch.as_u64() as i64))
            .filter(validator_nodes::public_key.eq(ByteArray::as_bytes(public_key)))
            .order_by(validator_nodes::shard_key.desc())
            .first::<DbValidatorNode>(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "get::validator_node".to_string(),
            })?;

        let vn = vn.try_into()?;
        Ok(vn)
    }

    fn validator_nodes_count(&self, tx: &mut Self::DbTransaction<'_>, epoch: Epoch) -> Result<u64, Self::Error> {
        let count = sql_query(
            "SELECT COUNT(distinct public_key) as cnt FROM validator_nodes WHERE start_epoch <= ? AND (end_epoch IS \
             NULL OR end_epoch > ?)",
        )
        .bind::<BigInt, _>(epoch.as_u64() as i64)
        .bind::<BigInt, _>(epoch.as_u64() as i64)
        .get_result::<Count>(tx.connection())
        .map_err(|source| SqliteStorageError::DieselError {
            source,
            operation: "count_validator_nodes".to_string(),
        })?;

        Ok(count.cnt as u64)
    }

    fn validator_nodes_count_for_shard_group(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        epoch: Epoch,
        shard_group: ShardGroup,
    ) -> Result<u64, Self::Error> {
        use crate::global::schema::committees;

        let count = committees::table
            .filter(committees::epoch.eq(epoch.as_u64() as i64))
            .filter(committees::shard_start.eq(shard_group.start().as_u32() as i32))
            .filter(committees::shard_end.eq(shard_group.end().as_u32() as i32))
            .count()
            .get_result::<i64>(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "count_validator_nodes".to_string(),
            })?;

        Ok(count as u64)
    }

    fn validator_nodes_get_committees_for_epoch(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        epoch: Epoch,
    ) -> Result<HashMap<ShardGroup, Committee<Self::Addr>>, Self::Error> {
        use crate::global::schema::{committees, validator_nodes};

        let results = committees::table
            .inner_join(validator_nodes::table.on(committees::validator_node_id.eq(validator_nodes::id)))
            .select((
                committees::shard_start,
                committees::shard_end,
                validator_nodes::address,
                validator_nodes::public_key,
            ))
            .filter(committees::epoch.eq(epoch.as_u64() as i64))
            .load::<(i32, i32, String, Vec<u8>)>(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "validator_nodes_get_committees".to_string(),
            })?;

        let mut committees = HashMap::new();
        for (shard_start, shard_end, address, public_key) in results {
            let addr = DbValidatorNode::try_parse_address(&address)?;
            let pk = PublicKey::from_canonical_bytes(&public_key)
                .map_err(|_| SqliteStorageError::MalformedDbData("Invalid public key".to_string()))?;
            committees
                .entry(ShardGroup::new(shard_start as u32, shard_end as u32))
                .or_insert_with(Committee::empty)
                .members
                .push((addr, pk));
        }

        Ok(committees)
    }

    fn validator_nodes_set_committee_shard(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        shard_key: SubstateAddress,
        shard_group: ShardGroup,
        epoch: Epoch,
    ) -> Result<(), Self::Error> {
        use crate::global::schema::{committees, validator_nodes};
        // This is probably not the most robust way of doing this. Ideally you would pass the validator ID to the
        // function and use that to insert into the committees table.
        let validator_id = validator_nodes::table
            .select(validator_nodes::id)
            .filter(validator_nodes::shard_key.eq(shard_key.as_bytes()))
            .filter(validator_nodes::start_epoch.le(epoch.as_u64() as i64))
            .order_by(validator_nodes::id.desc())
            .first::<i32>(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "validator_nodes_set_committee_bucket".to_string(),
            })?;

        diesel::insert_into(committees::table)
            .values((
                committees::validator_node_id.eq(validator_id),
                committees::epoch.eq(epoch.as_u64() as i64),
                committees::shard_start.eq(shard_group.start().as_u32() as i32),
                committees::shard_end.eq(shard_group.end().as_u32() as i32),
            ))
            .execute(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "insert::committee_bucket".to_string(),
            })?;
        Ok(())
    }

    fn validator_nodes_get_for_shard_group(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        epoch: Epoch,
        shard_group: ShardGroup,
        shuffle: bool,
        limit: usize,
    ) -> Result<Committee<Self::Addr>, Self::Error> {
        use crate::global::schema::{committees, validator_nodes};

        let mut query = validator_nodes::table
            .inner_join(committees::table.on(committees::validator_node_id.eq(validator_nodes::id)))
            .select(validator_nodes::all_columns)
            .filter(committees::epoch.eq(epoch.as_u64() as i64))
            .filter(committees::shard_start.eq(shard_group.start().as_u32() as i32))
            .filter(committees::shard_end.eq(shard_group.end().as_u32() as i32))
            .into_boxed();

        if shuffle {
            query = query.order_by(sql_random());
        }

        let validators = query
            .limit(i64::try_from(limit).unwrap_or(i64::MAX))
            .get_results::<DbValidatorNode>(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "validator_nodes_get_for_shard_group".to_string(),
            })?;

        debug!(target: LOG_TARGET, "Found {} validators", validators.len());

        validators
            .into_iter()
            .map(|vn| {
                Ok((
                    DbValidatorNode::try_parse_address(&vn.address)?,
                    PublicKey::from_canonical_bytes(&vn.public_key).map_err(|_| {
                        SqliteStorageError::MalformedDbData(format!(
                            "validator_nodes_get_for_shard_group: Invalid public key in validator node record id={}",
                            vn.id
                        ))
                    })?,
                ))
            })
            .collect()
    }

    fn validator_nodes_get_overlapping_shard_group(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        epoch: Epoch,
        shard_group: ShardGroup,
    ) -> Result<HashMap<ShardGroup, Committee<Self::Addr>>, Self::Error> {
        use crate::global::schema::{committees, validator_nodes};

        let validators = validator_nodes::table
            .inner_join(committees::table.on(committees::validator_node_id.eq(validator_nodes::id)))
            .select((validator_nodes::all_columns, committees::all_columns))
            .filter(committees::epoch.eq(epoch.as_u64() as i64))
            // Overlapping c.shard_start <= :end and c.shard_end >= :start;
            .filter(committees::shard_start.le(shard_group.end().as_u32() as i32))
            .filter(committees::shard_end.ge(shard_group.start().as_u32() as i32))
            .get_results::<(DbValidatorNode, DbCommittee)>(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "validator_nodes_get_overlapping_shard_group".to_string(),
            })?;

        debug!(target: LOG_TARGET, "Found {} validators", validators.len());

        let mut committees = HashMap::with_capacity(shard_group.len());
        for (vn, committee) in validators {
            let validators = committees
                .entry(committee.as_shard_group())
                .or_insert_with(|| Committee::empty());

            validators.members.push((
                DbValidatorNode::try_parse_address(&vn.address)?,
                PublicKey::from_canonical_bytes(&vn.public_key).map_err(|_| {
                    SqliteStorageError::MalformedDbData(format!(
                        "validator_nodes_get_overlapping_shard_group: Invalid public key in validator node record \
                         id={}",
                        vn.id
                    ))
                })?,
            ));
        }

        Ok(committees)
    }

    fn validator_nodes_get_random_committee_member_from_shard_group(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        epoch: Epoch,
        shard_group: Option<ShardGroup>,
        excluding: Vec<Self::Addr>,
    ) -> Result<ValidatorNode<Self::Addr>, Self::Error> {
        use crate::global::schema::{committees, validator_nodes};

        let mut query = validator_nodes::table
            .inner_join(committees::table.on(validator_nodes::id.eq(committees::validator_node_id)))
            .select(validator_nodes::all_columns)
            .filter(committees::epoch.eq(epoch.as_u64() as i64))
            .filter(validator_nodes::address.ne_all(excluding.into_iter().map(|a| a.to_string())))
            .order_by(sql_random())
            .into_boxed();

        if let Some(shard_group) = shard_group {
            query = query
                .filter(committees::shard_start.eq(shard_group.start().as_u32() as i32))
                .filter(committees::shard_end.eq(shard_group.end().as_u32() as i32));
        }

        let vn = query
            .first::<DbValidatorNode>(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "get::validator_node".to_string(),
            })?;

        let vn = vn.try_into()?;
        Ok(vn)
    }

    fn get_validator_nodes_within_start_epoch(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        epoch: Epoch,
    ) -> Result<Vec<ValidatorNode<Self::Addr>>, Self::Error> {
        use crate::global::schema::validator_nodes;

        let sqlite_vns = validator_nodes::table
            .filter(validator_nodes::start_epoch.le(epoch.as_u64() as i64))
            .filter(
                validator_nodes::end_epoch
                    .is_null()
                    .or(validator_nodes::end_epoch.gt(epoch.as_u64() as i64)),
            )
            .get_results::<DbValidatorNode>(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: format!("get::get_validator_nodes_within_epochs({})", epoch),
            })?;

        distinct_validators_sorted(sqlite_vns)
    }

    fn get_validator_nodes_within_committee_epoch(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        epoch: Epoch,
    ) -> Result<Vec<ValidatorNode<Self::Addr>>, Self::Error> {
        use crate::global::schema::{committees, validator_nodes};

        let sqlite_vns = validator_nodes::table
            .select(validator_nodes::all_columns)
            .inner_join(committees::table.on(validator_nodes::id.eq(committees::validator_node_id)))
            .filter(committees::epoch.eq(epoch.as_u64() as i64))
            .order_by(validator_nodes::shard_key.asc())
            .get_results::<DbValidatorNode>(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: format!("get::get_validator_nodes_within_epochs({})", epoch),
            })?;

        sqlite_vns.into_iter().map(TryInto::try_into).collect()
    }

    fn insert_epoch(&self, tx: &mut Self::DbTransaction<'_>, epoch: DbEpoch) -> Result<(), Self::Error> {
        use crate::global::schema::epochs;

        let sqlite_epoch: NewEpoch = epoch.into();

        diesel::insert_into(epochs::table)
            .values(&sqlite_epoch)
            .execute(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "insert::epoch".to_string(),
            })?;

        Ok(())
    }

    fn get_epoch(&self, tx: &mut Self::DbTransaction<'_>, epoch: u64) -> Result<Option<DbEpoch>, Self::Error> {
        use crate::global::schema::epochs::dsl;

        let query_res: Option<models::Epoch> = dsl::epochs
            .find(epoch as i64)
            .first(tx.connection())
            .optional()
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "get::epoch".to_string(),
            })?;

        match query_res {
            Some(e) => Ok(Some(e.into())),
            None => Ok(None),
        }
    }

    fn insert_base_layer_block_info(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        info: DbBaseLayerBlockInfo,
    ) -> Result<(), Self::Error> {
        use crate::global::schema::base_layer_block_info;
        let sqlite_base_layer_block_info: NewBaseLayerBlockInfo = info.into();

        diesel::insert_into(base_layer_block_info::table)
            .values(&sqlite_base_layer_block_info)
            .on_conflict_do_nothing()
            .execute(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "insert::base_layer_block_info".to_string(),
            })?;

        Ok(())
    }

    fn get_base_layer_block_info(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        hash: FixedHash,
    ) -> Result<Option<DbBaseLayerBlockInfo>, Self::Error> {
        use crate::global::schema::base_layer_block_info::dsl;
        let query_res: Option<models::BaseLayerBlockInfo> = dsl::base_layer_block_info
            .filter(dsl::hash.eq(hash.to_vec()))
            .first(tx.connection())
            .optional()
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "get::base_layer_block_info".to_string(),
            })?;
        match query_res {
            Some(e) => Ok(Some(e.try_into()?)),
            None => Ok(None),
        }
    }

    fn insert_bmt(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        epoch: u64,
        bmt: ValidatorNodeBalancedMerkleTree,
    ) -> Result<(), Self::Error> {
        use crate::global::schema::bmt_cache;

        diesel::insert_into(bmt_cache::table)
            .values((
                bmt_cache::epoch.eq(epoch as i64),
                bmt_cache::bmt.eq(serde_json::to_vec(&bmt)?),
            ))
            .execute(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "insert::bmt".to_string(),
            })?;

        Ok(())
    }

    fn get_bmt(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        epoch: Epoch,
    ) -> Result<Option<ValidatorNodeBalancedMerkleTree>, Self::Error> {
        use crate::global::schema::bmt_cache::dsl;

        let query_res: Option<models::Bmt> = dsl::bmt_cache
            .find(epoch.as_u64() as i64)
            .first(tx.connection())
            .optional()
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "get::bmt".to_string(),
            })?;
        match query_res {
            Some(bmt) => Ok(Some(serde_json::from_slice(&bmt.bmt)?)),
            None => Ok(None),
        }
    }

    fn insert_layer_one_transaction<T: Serialize>(
        &self,
        tx: &mut Self::DbTransaction<'_>,
        data: DbLayer1Transaction<T>,
    ) -> Result<(), Self::Error> {
        use crate::global::schema::layer_one_transactions;

        diesel::insert_into(layer_one_transactions::table)
            .values((
                layer_one_transactions::epoch.eq(data.epoch.as_u64() as i64),
                layer_one_transactions::payload_type.eq(data.proof_type.to_string()),
                layer_one_transactions::payload.eq(serde_json::to_string_pretty(&data.payload)?),
            ))
            .execute(tx.connection())
            .map_err(|source| SqliteStorageError::DieselError {
                source,
                operation: "insert::layer_one_transaction".to_string(),
            })?;

        Ok(())
    }
}

impl<TAddr> Debug for SqliteGlobalDbAdapter<TAddr> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteGlobalDbAdapter")
            .field("db", &"Arc<Mutex<SqliteConnection>>")
            .finish()
    }
}

impl<TAddr> Clone for SqliteGlobalDbAdapter<TAddr> {
    fn clone(&self) -> Self {
        Self {
            connection: self.connection.clone(),
            _addr: PhantomData,
        }
    }
}

fn distinct_validators<TAddr: NodeAddressable>(
    mut sqlite_vns: Vec<DbValidatorNode>,
) -> Result<Vec<ValidatorNode<TAddr>>, SqliteStorageError> {
    // first, sort by registration block height so that we get newer registrations first
    let mut db_vns = Vec::with_capacity(sqlite_vns.len());
    sqlite_vns.sort_by(|a, b| a.start_epoch.cmp(&b.start_epoch).reverse());
    let mut dedup_map = HashSet::<Vec<u8>>::with_capacity(sqlite_vns.len());
    for vn in sqlite_vns {
        if !dedup_map.contains(&vn.public_key) {
            dedup_map.insert(vn.public_key.clone());
            db_vns.push(ValidatorNode::try_from(vn)?);
        }
    }

    Ok(db_vns)
}

fn distinct_validators_sorted<TAddr: NodeAddressable>(
    sqlite_vns: Vec<DbValidatorNode>,
) -> Result<Vec<ValidatorNode<TAddr>>, SqliteStorageError> {
    let mut db_vns = distinct_validators(sqlite_vns)?;
    db_vns.sort_by(|a, b| a.shard_key.cmp(&b.shard_key));
    Ok(db_vns)
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    cnt: i64,
}
