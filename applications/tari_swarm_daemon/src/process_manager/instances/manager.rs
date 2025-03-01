//   Copyright 2024 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::{
    collections::HashMap,
    io::BufReader as StdBufReader,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    process::{ExitStatus, Stdio},
    time::Duration,
};

use anyhow::{anyhow, Context};
use log::info;
use slug::slugify;
use tari_common::configuration::Network;
use tokio::{
    fs,
    fs::File,
    io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader},
    task,
    time::sleep,
};

use super::InstanceId;
use crate::{
    config::{InstanceConfig, InstanceType},
    process_definitions::{get_definition, ProcessContext},
    process_manager::{
        executables::{Executable, Executables},
        port_allocator::PortAllocator,
        processes::{MinoTariMinerProcess, MinoTariNodeProcess, MinoTariWalletProcess, ValidatorNodeProcess},
        AllocatedPorts,
        IndexerProcess,
        Instance,
        SignalingServerProcess,
        WalletDaemonProcess,
    },
};

pub struct InstanceManager {
    base_path: PathBuf,
    config: Vec<InstanceConfig>,
    global_settings: HashMap<String, String>,
    network: Network,
    minotari_nodes: HashMap<InstanceId, MinoTariNodeProcess>,
    minotari_wallets: HashMap<InstanceId, MinoTariWalletProcess>,
    minotari_miners: HashMap<InstanceId, MinoTariMinerProcess>,
    validator_nodes: HashMap<InstanceId, ValidatorNodeProcess>,
    indexers: HashMap<InstanceId, IndexerProcess>,
    wallet_daemons: HashMap<InstanceId, WalletDaemonProcess>,
    signaling_servers: HashMap<InstanceId, SignalingServerProcess>,
    port_allocator: PortAllocator,
    instance_id: InstanceId,
}

impl InstanceManager {
    pub fn new(
        base_path: PathBuf,
        network: Network,
        global_settings: HashMap<String, String>,
        config: Vec<InstanceConfig>,
        start_port: u16,
    ) -> Self {
        Self {
            base_path,
            config,
            network,
            global_settings,
            minotari_nodes: HashMap::new(),
            minotari_wallets: HashMap::new(),
            minotari_miners: HashMap::new(),
            validator_nodes: HashMap::new(),
            indexers: HashMap::new(),
            wallet_daemons: HashMap::new(),
            signaling_servers: HashMap::new(),
            port_allocator: PortAllocator::new(start_port),
            instance_id: 0,
        }
    }

    /// Fork all defined processes in order
    pub async fn fork_all(&mut self, executables: Executables<'_>) -> anyhow::Result<()> {
        for mut instance in self.config.clone() {
            let executable = executables.get(instance.execution_instance_type()).ok_or_else(|| {
                anyhow!(
                    "No executable found for instance type '{}'. This is a bug in the configuration",
                    instance.execution_instance_type()
                )
            })?;

            let mut settings = self.global_settings.clone();
            settings.extend(instance.settings.drain());
            for i in 0..instance.num_instances {
                self.fork_new(
                    executable,
                    instance.instance_type,
                    instance.instance_name(i),
                    instance.base_path_override().cloned(),
                    settings.clone(),
                )
                .await?;
            }
        }
        Ok(())
    }

    pub async fn fork_new(
        &mut self,
        executable: &Executable,
        instance_type: InstanceType,
        instance_name: String,
        base_path_override: Option<PathBuf>,
        settings: HashMap<String, String>,
    ) -> anyhow::Result<InstanceId> {
        let instance_id = self.next_instance_id();
        self.fork(
            instance_id,
            executable,
            instance_type,
            instance_name,
            base_path_override,
            settings,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_lines)]
    async fn fork(
        &mut self,
        instance_id: InstanceId,
        executable: &Executable,
        instance_type: InstanceType,
        instance_name: String,
        base_path_override: Option<PathBuf>,
        mut instance_settings: HashMap<String, String>,
        ports: Option<AllocatedPorts>,
    ) -> anyhow::Result<InstanceId> {
        let listen_ip = instance_settings
            .get("listen_ip")
            .map(|s| s.parse())
            .transpose()
            .context("Failed to parse listen_ip arg")?
            .unwrap_or_else(|| IpAddr::V4(Ipv4Addr::from([127, 0, 0, 1])));
        let definition = get_definition(instance_type);

        log::info!(
            "🚀 Starting {} (id: {}, exec path: {}, listen_ip: {})",
            instance_type,
            instance_id,
            executable.path.display(),
            listen_ip
        );

        let mut allocated_ports = ports.unwrap_or_else(|| self.port_allocator.create());

        let processes_path = self.base_path.join("processes");
        let base_path = match base_path_override {
            Some(base_path) => {
                if base_path.is_absolute() {
                    base_path
                } else {
                    processes_path.join(base_path)
                }
            },
            None => processes_path.join(slugify(&instance_name)),
        };
        fs::create_dir_all(&base_path).await.context("create_dir_all in fork")?;

        // Special handling to set the claim public key if we find a file containing one
        if instance_type.is_validator() {
            let claim_public_key_file = processes_path.join("claim_key.json");
            if claim_public_key_file.exists() {
                let file = File::open(&claim_public_key_file)
                    .await
                    .context("Failed to open claim public key file")?;
                let file = file.into_std().await;
                let reader = StdBufReader::new(file);
                let claim_public_key = serde_json::from_reader::<_, serde_json::Value>(reader)
                    .context("Failed to read claim public key file")?;
                let claim_public_key = claim_public_key
                    .get("public_key")
                    .and_then(|pk| pk.as_str())
                    .context("Failed to extract public key from claim public key file")?;
                info!("Setting claim public key to {}", claim_public_key);
                instance_settings.insert("claim_public_key".to_string(), claim_public_key.to_string());
            }
        }

        let context = ProcessContext::new(
            instance_id,
            &executable.path,
            &executable.env,
            base_path.clone(),
            processes_path,
            self.network,
            listen_ip,
            &mut allocated_ports,
            self,
            &instance_settings,
        );
        if !context.bin().exists() {
            return Err(anyhow::anyhow!(
                "{} binary not found at {}",
                instance_type,
                context.bin().display()
            ));
        }

        let mut command = definition.get_command(context).await.context("get_command")?;
        let stdout_log_path = base_path.join("stdout.log");
        let stderr_log_path = base_path.join("stderr.log");
        command
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // Any attempt to use stdin will fail immediately
            .stdin(Stdio::null());
        info!("Command: {:?}", command);
        let mut child = command.spawn().with_context(|| format!("spawn {instance_type}"))?;

        self.port_allocator.register(instance_id, allocated_ports.clone());

        if let Some(stdout) = child.stdout.take() {
            forward_logs(stdout_log_path, stdout, instance_name.clone());
        }
        if let Some(stderr) = child.stderr.take() {
            forward_logs(stderr_log_path, stderr, instance_name.clone());
        }

        let mut instance = Instance::new_started(
            instance_id,
            instance_name,
            instance_type,
            child,
            allocated_ports,
            // This saves us from having to join the network string to the path all over the place, since everything we
            // want is under {base_dir}/{network}
            base_path.join(self.network.to_string()),
            instance_settings,
        );

        // Wait for base layer nodes to start
        if instance_type.is_base_layer_node() {
            sleep(Duration::from_secs(2)).await;
            instance.check_running().context("Failed to start instance")?;
        }

        log::info!(
            "🟢 Started {} (id: {}, path: {}, pid: {:?})",
            instance_type,
            instance_id,
            executable.path.display(),
            instance.child().id()
        );

        match instance_type {
            InstanceType::MinoTariNode => {
                self.minotari_nodes
                    .insert(instance_id, MinoTariNodeProcess::new(instance));
            },
            InstanceType::MinoTariConsoleWallet => {
                self.minotari_wallets
                    .insert(instance_id, MinoTariWalletProcess::new(instance));
            },
            InstanceType::MinoTariMiner => {
                self.minotari_miners
                    .insert(instance_id, MinoTariMinerProcess::new(instance));
            },
            InstanceType::TariValidatorNode => {
                self.validator_nodes
                    .insert(instance_id, ValidatorNodeProcess::new(instance));
            },
            InstanceType::TariIndexer => {
                self.indexers.insert(instance_id, IndexerProcess::new(instance));
            },
            InstanceType::TariSignalingServer => {
                self.signaling_servers
                    .insert(instance_id, SignalingServerProcess::new(instance));
            },
            InstanceType::TariWalletDaemon => {
                self.wallet_daemons
                    .insert(instance_id, WalletDaemonProcess::new(instance));
            },
            InstanceType::TariWalletDaemonCreateKey => {
                self.wallet_daemons
                    .insert(instance_id, WalletDaemonProcess::new(instance));
            },
        }

        Ok(instance_id)
    }

    pub fn minotari_nodes(&self) -> impl Iterator<Item = &MinoTariNodeProcess> + Sized {
        self.minotari_nodes.values()
    }

    pub fn minotari_wallets(&self) -> impl Iterator<Item = &MinoTariWalletProcess> + Sized {
        self.minotari_wallets.values()
    }

    pub fn validator_nodes(&self) -> impl Iterator<Item = &ValidatorNodeProcess> + Sized {
        self.validator_nodes.values()
    }

    pub fn num_instances(&self) -> usize {
        self.instances().count()
    }

    pub fn num_validator_nodes(&self) -> u64 {
        self.validator_nodes.len() as u64
    }

    pub fn validator_nodes_mut(&mut self) -> impl Iterator<Item = &mut ValidatorNodeProcess> + Sized {
        self.validator_nodes.values_mut()
    }

    // pub fn minotari_miners(&self) -> impl Iterator<Item = &MinoTariMinerProcess> + Sized {
    //     self.minotari_miners.values()
    // }

    pub fn indexers(&self) -> impl Iterator<Item = &IndexerProcess> + Sized {
        self.indexers.values()
    }

    pub fn signaling_servers(&self) -> impl Iterator<Item = &SignalingServerProcess> + Sized {
        self.signaling_servers.values()
    }

    // pub fn wallet_daemons(&self) -> impl Iterator<Item = &WalletDaemonProcess> + Sized {
    //     self.wallet_daemons.values()
    // }

    pub fn get_wallet_daemon_mut(&mut self, id: InstanceId) -> Option<&mut WalletDaemonProcess> {
        self.wallet_daemons.get_mut(&id)
    }

    pub fn get_instance_mut(&mut self, id: InstanceId) -> Option<&mut Instance> {
        self.instances_mut().find(|i| i.id() == id)
    }

    pub async fn wait(&mut self, id: InstanceId) -> anyhow::Result<ExitStatus> {
        let instance = self.get_instance_mut(id).ok_or_else(|| anyhow!("Instance not found"))?;
        let status = instance.child_mut().wait().await?;
        Ok(status)
    }

    pub async fn start_instance(&mut self, id: InstanceId, executable: &Executable) -> anyhow::Result<()> {
        let instance = self
            .instances()
            .find(|i| i.id() == id)
            .ok_or_else(|| anyhow!("Instance not found"))?;

        let instance_type = instance.instance_type();
        let instance_name = instance.name().to_string();
        let settings = instance.settings().clone();
        let ports = instance.allocated_ports().clone();

        // This will just overwrite the previous instance
        self.fork(
            id,
            executable,
            instance_type,
            instance_name,
            None,
            settings,
            Some(ports),
        )
        .await?;

        Ok(())
    }

    pub async fn stop_instance(&mut self, id: InstanceId) -> anyhow::Result<()> {
        let instance = self
            .instances_mut()
            .find(|i| i.id() == id)
            .ok_or_else(|| anyhow!("Instance not found"))?;

        instance.terminate().await?;
        instance.check_running()?;
        Ok(())
    }

    pub async fn delete_instance_data(&mut self, id: InstanceId) -> anyhow::Result<()> {
        let instance = self
            .instances_mut()
            .find(|i| i.id() == id)
            .ok_or_else(|| anyhow!("Instance not found"))?;

        let definition = get_definition(instance.instance_type());

        if let Some(data_path) = definition.get_relative_data_path() {
            let path = instance.base_path().join(data_path);
            info!(
                "Deleting data directory for instance {}: {}",
                instance.name(),
                path.display()
            );
            fs::remove_dir_all(path).await?;
        }
        Ok(())
    }

    pub fn instances_mut(&mut self) -> impl Iterator<Item = &mut Instance> {
        self.minotari_nodes
            .values_mut()
            .map(|x| x.instance_mut())
            .chain(self.minotari_wallets.values_mut().map(|x| x.instance_mut()))
            .chain(self.minotari_miners.values_mut().map(|x| x.instance_mut()))
            .chain(self.validator_nodes.values_mut().map(|x| x.instance_mut()))
            .chain(self.indexers.values_mut().map(|x| x.instance_mut()))
            .chain(self.signaling_servers.values_mut().map(|x| x.instance_mut()))
            .chain(self.wallet_daemons.values_mut().map(|x| x.instance_mut()))
    }

    pub fn instances(&self) -> impl Iterator<Item = &Instance> {
        self.minotari_nodes
            .values()
            .map(|x| x.instance())
            .chain(self.minotari_wallets.values().map(|x| x.instance()))
            .chain(self.minotari_miners.values().map(|x| x.instance()))
            .chain(self.validator_nodes.values().map(|x| x.instance()))
            .chain(self.indexers.values().map(|x| x.instance()))
            .chain(self.signaling_servers.values().map(|x| x.instance()))
            .chain(self.wallet_daemons.values().map(|x| x.instance()))
    }

    fn next_instance_id(&mut self) -> InstanceId {
        let id = self.instance_id;
        self.instance_id += 1;
        id
    }
}

fn forward_logs<R: AsyncRead + Unpin + Send + 'static>(path: PathBuf, reader: R, target: String) {
    let mut lines = BufReader::new(reader).lines();
    task::spawn(async move {
        let mut log_file = File::create(path).await.unwrap();
        while let Some(output) = lines.next_line().await.unwrap() {
            log::debug!(target: "swarm", "[{target}] {output}");
            log_file.write_all(output.as_bytes()).await.unwrap();
            log_file.write_all(b"\n").await.unwrap();
            log_file.flush().await.unwrap();
        }
        log::debug!(target: "swarm", "Process exited ({target})");
    });
}
