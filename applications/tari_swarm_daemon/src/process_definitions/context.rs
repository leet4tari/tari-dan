//   Copyright 2024 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{collections::HashMap, net::IpAddr, path::PathBuf};

use tari_common::configuration::Network;

use crate::process_manager::{
    AllocatedPorts,
    IndexerProcess,
    InstanceId,
    InstanceManager,
    MinoTariNodeProcess,
    MinoTariWalletProcess,
    SignalingServerProcess,
};

pub struct ProcessContext<'a> {
    instance_id: InstanceId,
    bin: &'a PathBuf,
    envs: &'a [(String, String)],
    base_path: PathBuf,
    processes_path: PathBuf,
    network: Network,
    listen_ip: IpAddr,
    port_allocator: &'a mut AllocatedPorts,
    instances: &'a InstanceManager,
    settings: &'a HashMap<String, String>,
}

impl<'a> ProcessContext<'a> {
    pub(crate) fn new(
        instance_id: InstanceId,
        bin: &'a PathBuf,
        envs: &'a [(String, String)],
        base_path: PathBuf,
        processes_path: PathBuf,
        network: Network,
        listen_ip: IpAddr,
        port_allocator: &'a mut AllocatedPorts,
        instances: &'a InstanceManager,
        settings: &'a HashMap<String, String>,
    ) -> Self {
        Self {
            instance_id,
            bin,
            envs,
            base_path,
            processes_path,
            network,
            listen_ip,
            port_allocator,
            instances,
            settings,
        }
    }

    pub fn instance_id(&self) -> InstanceId {
        self.instance_id
    }

    pub fn bin(&self) -> &PathBuf {
        self.bin
    }

    pub fn base_path(&self) -> &PathBuf {
        &self.base_path
    }

    pub fn processes_path(&self) -> &PathBuf {
        &self.processes_path
    }

    pub fn network(&self) -> Network {
        self.network
    }

    pub fn get_setting(&self, key: &str) -> Option<&str> {
        self.settings.get(key).map(|s| s.as_str())
    }

    pub async fn get_free_port(&mut self, name: &'static str) -> anyhow::Result<u16> {
        Ok(self.port_allocator.get_or_next_port(name).await)
    }

    pub fn listen_ip(&self) -> &IpAddr {
        &self.listen_ip
    }

    pub fn environment(&self) -> impl Iterator<Item = (&'a str, &'a str)> {
        self.envs.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    pub fn minotari_nodes(&self) -> impl Iterator<Item = &MinoTariNodeProcess> {
        self.instances.minotari_nodes()
    }

    pub fn minotari_wallets(&self) -> impl Iterator<Item = &MinoTariWalletProcess> {
        self.instances.minotari_wallets()
    }

    pub fn indexers(&self) -> impl Iterator<Item = &IndexerProcess> {
        self.instances.indexers()
    }

    pub fn signaling_servers(&self) -> impl Iterator<Item = &SignalingServerProcess> {
        self.instances.signaling_servers()
    }
}
