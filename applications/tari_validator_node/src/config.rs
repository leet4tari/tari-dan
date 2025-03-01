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

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use config::Config;
use libp2p::Multiaddr;
use serde::{Deserialize, Serialize};
use tari_common::{
    configuration::{serializers, CommonConfig, Network},
    ConfigurationError,
    DefaultConfigLoader,
    SubConfigPath,
};
use tari_crypto::ristretto::RistrettoPublicKey;
use tari_dan_app_utilities::p2p_config::{P2pConfig, PeerSeedsConfig, RpcConfig};
use tari_template_manager::implementation::TemplateConfig;
use url::Url;

#[derive(Debug, Clone)]
pub struct ApplicationConfig {
    pub common: CommonConfig,
    pub validator_node: ValidatorNodeConfig,
    pub peer_seeds: PeerSeedsConfig,
    pub network: Network,
}

impl ApplicationConfig {
    pub fn load_from(cfg: &Config) -> Result<Self, ConfigurationError> {
        let mut config = Self {
            common: CommonConfig::load_from(cfg)?,
            validator_node: ValidatorNodeConfig::load_from(cfg)?,
            peer_seeds: PeerSeedsConfig::load_from(cfg)?,
            network: cfg.get("network")?,
        };
        config.validator_node.set_base_path(config.common.base_path());
        Ok(config)
    }

    pub fn get_layer_one_transaction_base_path(&self) -> PathBuf {
        if self.validator_node.layer_one_transaction_path.is_absolute() {
            return self.validator_node.layer_one_transaction_path.clone();
        }
        self.common
            .base_path()
            .join(&self.validator_node.layer_one_transaction_path)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)]
pub struct ValidatorNodeConfig {
    override_from: Option<String>,
    pub shard_key_file: PathBuf,
    /// A path to the file that stores your node identity and secret key
    pub identity_file: PathBuf,
    //// The node's publicly-accessible hostname
    // pub public_address: Option<Multiaddr>,
    /// The Tari base node's GRPC URL
    pub base_node_grpc_url: Option<Url>,
    /// If set to false, there will be no base layer scanning at all
    pub scan_base_layer: bool,
    /// How often do we want to scan the base layer for changes
    #[serde(with = "serializers::seconds")]
    pub base_layer_scanning_interval: Duration,
    /// The relative path to store persistent data
    pub data_dir: PathBuf,
    /// The p2p configuration settings
    pub p2p: P2pConfig,
    /// P2P RPC configuration
    pub rpc: RpcConfig,
    /// GRPC address of the validator node  application
    pub grpc_address: Option<Multiaddr>,
    /// JSON-RPC address of the validator node  application
    pub json_rpc_listener_address: Option<SocketAddr>,
    /// The jrpc address where the UI should connect (it can be the same as the json_rpc_address, but doesn't have to
    /// be), if this will be None, then the listen_addr will be used.
    pub json_rpc_public_address: Option<String>,
    /// The address of the HTTP UI
    pub http_ui_listener_address: Option<SocketAddr>,
    /// Template config
    pub templates: TemplateConfig,
    /// Fee claim public key
    pub fee_claim_public_key: RistrettoPublicKey,
    /// Create identity file if not exists
    pub dont_create_id: bool,
    /// The (optional) sidechain to run this on
    pub validator_node_sidechain_id: Option<RistrettoPublicKey>,
    /// The templates sidechain id
    pub template_sidechain_id: Option<RistrettoPublicKey>,
    /// The burnt utxo sidechain id
    pub burnt_utxo_sidechain_id: Option<RistrettoPublicKey>,
    /// The path to store layer one transactions.
    pub layer_one_transaction_path: PathBuf,
}

impl ValidatorNodeConfig {
    pub fn state_db_path(&self) -> PathBuf {
        self.data_dir.join("state.db")
    }

    pub fn set_base_path<P: AsRef<Path>>(&mut self, base_path: P) {
        if !self.shard_key_file.is_absolute() {
            self.shard_key_file = base_path.as_ref().join(&self.shard_key_file);
        }
        if !self.identity_file.is_absolute() {
            self.identity_file = base_path.as_ref().join(&self.identity_file);
        }
        if !self.data_dir.is_absolute() {
            self.data_dir = base_path.as_ref().join(&self.data_dir);
        }
    }
}

impl Default for ValidatorNodeConfig {
    fn default() -> Self {
        Self {
            override_from: None,
            shard_key_file: PathBuf::from("shard_key.json"),
            identity_file: PathBuf::from("validator_node_id.json"),
            base_node_grpc_url: None,
            scan_base_layer: true,
            base_layer_scanning_interval: Duration::from_secs(10),
            data_dir: PathBuf::from("data/validator_node"),
            p2p: P2pConfig::default(),
            rpc: RpcConfig::default(),
            grpc_address: Some("/ip4/127.0.0.1/tcp/18144".parse().unwrap()),
            json_rpc_listener_address: Some("127.0.0.1:18200".parse().unwrap()),
            json_rpc_public_address: None,
            http_ui_listener_address: Some("127.0.0.1:5001".parse().unwrap()),
            templates: TemplateConfig::default(),
            // Burn your fees
            fee_claim_public_key: RistrettoPublicKey::default(),
            dont_create_id: false,
            validator_node_sidechain_id: None,
            template_sidechain_id: None,
            burnt_utxo_sidechain_id: None,
            layer_one_transaction_path: PathBuf::from("data/layer_one_transactions"),
        }
    }
}

impl SubConfigPath for ValidatorNodeConfig {
    fn main_key_prefix() -> &'static str {
        "validator_node"
    }
}
