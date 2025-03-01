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

use std::sync::Arc;

use log::{warn, *};
use tari_common::configuration::Network;
use tari_crypto::{range_proof::RangeProofService, ristretto::RistrettoPublicKey, tari_utilities::ByteArray};
use tari_dan_common_types::services::template_provider::TemplateProvider;
use tari_engine_types::{
    base_layer_hashing::ownership_proof_hasher64,
    commit_result::{FinalizeResult, RejectReason, TransactionResult},
    component::ComponentHeader,
    confidential::{get_commitment_factory, get_range_proof_service, ConfidentialClaim, ConfidentialOutput},
    entity_id_provider::EntityIdProvider,
    events::Event,
    hashing::hash_template_code,
    indexed_value::IndexedValue,
    instruction_result::InstructionResult,
    lock::LockFlag,
    logs::LogEntry,
    published_template::{PublishedTemplate, PublishedTemplateAddress},
    resource::Resource,
    resource_container::ResourceContainer,
    substate::{SubstateId, SubstateValue},
    vault::Vault,
    vn_fee_pool::ValidatorFeePoolAddress,
    TemplateAddress,
};
use tari_template_abi::{TemplateDef, Type};
use tari_template_builtin::{ACCOUNT_NFT_TEMPLATE_ADDRESS, ACCOUNT_TEMPLATE_ADDRESS};
use tari_template_lib::{
    args,
    args::{
        Arg,
        BucketAction,
        BucketRef,
        BuiltinTemplateAction,
        CallAction,
        CallFunctionArg,
        CallMethodArg,
        CallerContextAction,
        ComponentAction,
        ComponentRef,
        ConfidentialRevealArg,
        ConsensusAction,
        CreateComponentArg,
        CreateResourceArg,
        GenerateRandomAction,
        InvokeResult,
        LogLevel,
        MintResourceArg,
        NonFungibleAction,
        PayFeeArg,
        ProofAction,
        ProofRef,
        RecallResourceArg,
        ResourceAction,
        ResourceGetNonFungibleArg,
        ResourceRef,
        ResourceUpdateNonFungibleDataArg,
        VaultAction,
        VaultCreateProofByFungibleAmountArg,
        VaultCreateProofByNonFungiblesArg,
        VaultWithdrawArg,
        WorkspaceAction,
    },
    auth::{AuthHook, AuthHookCaller, ComponentAccessRules, OwnerRule, ResourceAccessRules, ResourceAuthAction},
    constants::{CONFIDENTIAL_TARI_RESOURCE_ADDRESS, XTR},
    crypto::RistrettoPublicKeyBytes,
    models::{
        Amount,
        BucketId,
        ComponentAddress,
        EntityId,
        Metadata,
        NonFungible,
        NonFungibleAddress,
        NotAuthorized,
        ResourceAddress,
        VaultId,
        VaultRef,
    },
    prelude::ResourceType,
    template::BuiltinTemplate,
};

use super::{working_state::WorkingState, Runtime};
use crate::{
    runtime::{
        engine_args::EngineArgs,
        error::AssertError,
        locking::{LockError, LockedSubstate},
        scope::PushCallFrame,
        tracker::StateTracker,
        utils::to_ristretto_public_key_bytes,
        RuntimeError,
        RuntimeInterface,
        RuntimeModule,
    },
    template::LoadedTemplate,
    transaction::TransactionProcessor,
};

const LOG_TARGET: &str = "tari::dan::engine::runtime::impl";

#[derive(Clone)]
pub struct RuntimeInterfaceImpl<TTemplateProvider> {
    tracker: StateTracker,
    template_provider: Arc<TTemplateProvider>,
    entity_id_provider: EntityIdProvider,
    transaction_signer_public_key: RistrettoPublicKey,
    modules: Vec<Arc<dyn RuntimeModule>>,
    max_call_depth: usize,
    network: Network,
}

impl<TTemplateProvider: TemplateProvider<Template = LoadedTemplate>> RuntimeInterfaceImpl<TTemplateProvider> {
    pub fn initialize(
        tracker: StateTracker,
        template_provider: Arc<TTemplateProvider>,
        signer_public_key: RistrettoPublicKey,
        entity_id_provider: EntityIdProvider,
        modules: Vec<Arc<dyn RuntimeModule>>,
        max_call_depth: usize,
        network: Network,
    ) -> Result<Self, RuntimeError> {
        let runtime = Self {
            tracker,
            template_provider,
            entity_id_provider,
            transaction_signer_public_key: signer_public_key,
            modules,
            max_call_depth,
            network,
        };
        runtime.invoke_modules_on_initialize()?;
        Ok(runtime)
    }

    fn invoke_modules_on_initialize(&self) -> Result<(), RuntimeError> {
        for module in &self.modules {
            module.on_initialize(&self.tracker)?;
        }
        Ok(())
    }

    fn invoke_modules_on_runtime_call(&self, function: &'static str) -> Result<(), RuntimeError> {
        for module in &self.modules {
            module.on_runtime_call(&self.tracker, function)?;
        }
        Ok(())
    }

    fn invoke_modules_on_before_finalize(&self) -> Result<(), RuntimeError> {
        for module in &self.modules {
            module.on_before_finalize(&self.tracker)?;
        }
        Ok(())
    }

    pub fn get_template_def(&self, template_address: &TemplateAddress) -> Result<TemplateDef, RuntimeError> {
        let loaded = self
            .template_provider
            .get_template_module(template_address)
            .map_err(|e| RuntimeError::FailedToLoadTemplate {
                address: *template_address,
                details: e.to_string(),
            })?
            .ok_or(RuntimeError::TemplateNotFound {
                template_address: *template_address,
            })?;

        Ok(loaded.template_def().clone())
    }

    fn validate_return_value(&self, value: &IndexedValue) -> Result<(), RuntimeError> {
        self.tracker.read_with(|state| {
            for bucket_id in value.bucket_ids() {
                let _ignore = state.get_bucket(*bucket_id)?;
            }

            for proof_id in value.proof_ids() {
                let _ignore = state.get_proof(*proof_id)?;
            }

            for id in value.referenced_substates() {
                if !state.substate_exists(&id)? {
                    debug!(
                        target: LOG_TARGET,
                        "Returned substate {id} does not exist",
                    );

                    return Err(RuntimeError::NonExistentSubstateReturned { id });
                }
            }

            Ok(())
        })
    }

    fn emit_vault_events<T: Into<String>>(
        &self,
        action: T,
        vault_id: VaultId,
        vault_lock: &LockedSubstate,
        amount: Amount,
        resource_type: ResourceType,
        state: &mut WorkingState,
    ) -> Result<(), RuntimeError> {
        let tx_hash = self.entity_id_provider.transaction_hash();
        let (&template_address, _) = state.current_template()?;
        let &resource_address = state.get_vault(vault_lock)?.resource_address();

        let payload = Metadata::from_iter([
            ("vault_id", vault_id.to_string()),
            ("resource_address", resource_address.to_string()),
            ("resource_type", resource_type.to_string()),
            ("amount", amount.to_string()),
        ]);

        let action = action.into();

        let vault_event = Event::std(
            Some(SubstateId::Vault(vault_id)),
            template_address,
            tx_hash,
            "vault",
            &action,
            payload,
        );
        debug!(target: LOG_TARGET, "Emitted vault event {}", vault_event);
        state.push_event(vault_event);

        Ok(())
    }

    fn invoke_resource_access_hook(
        &self,
        auth_hook: AuthHook,
        mut auth_caller: AuthHookCaller,
        action: ResourceAuthAction,
    ) -> Result<(), RuntimeError> {
        self.invoke_modules_on_runtime_call("invoke_resource_access_hook")?;
        // Check if the component exist
        let skip_hook = self.tracker.read_with(|state| {
            let current_component = state.current_component()?;
            // Only execute hooks if the resource is being acted upon by an external component
            if current_component == Some(auth_hook.component_address) {
                return Ok::<_, RuntimeError>(true);
            }
            // We know that the auth hook has been validated before this is called. However, the component may not yet
            // exist if it is being created in the same call as the resource action is taking place. For
            // example, commonly a user creates a resource with initial supply and deposits it into a bucket
            // before creating the component. In this case, we "skip" the hook.
            let exists = state.store().exists(&auth_hook.component_address.into())?;
            Ok::<_, RuntimeError>(!exists)
        })?;

        if skip_hook {
            return Ok(());
        }

        let caller = auth_caller
            .component()
            .map(|component| self.load_component(component))
            .transpose()?;

        if let Some(caller) = caller {
            auth_caller.with_component_state(caller.into_component().state);
        }

        // The signature of a call back is (action: ResourceAuthAction, auth_caller: AuthCaller)
        let ret = self
            .invoke_component_method(&auth_hook.component_address, &auth_hook.method, args![
                action,
                auth_caller
            ])
            .map_err(|e| match e {
                RuntimeError::CrossTemplateCallMethodError { details, .. } => RuntimeError::AccessDeniedAuthHook {
                    action_ident: action.into(),
                    details: details.to_string(),
                },
                _ => e,
            })?;
        // Enforce that the return type is actually empty. We cannot rely on InstructionResult::return_type field
        // because that comes from the template definition which is defined by the template author and may not reflect
        // actual behaviour.
        if !ret.indexed.value().is_null() {
            return Err(RuntimeError::UnexpectedNonNullInAuthHookReturn);
        }
        Ok(())
    }

    fn invoke_component_method(
        &self,
        component_address: &ComponentAddress,
        method: &str,
        args: Vec<Arg>,
    ) -> Result<InstructionResult, RuntimeError> {
        let call_runtime = Runtime::new(Arc::new(self.clone()));
        TransactionProcessor::call_method(&*self.template_provider, &call_runtime, component_address, method, args)
            .map_err(|e| RuntimeError::CrossTemplateCallMethodError {
                component_address: *component_address,
                method: method.to_string(),
                details: e.to_string(),
            })
    }

    fn invoke_template_function(
        &self,
        template_address: &TemplateAddress,
        function: &str,
        args: Vec<Arg>,
    ) -> Result<InstructionResult, RuntimeError> {
        // we are initializing a new runtime for the nested call
        let call_runtime = Runtime::new(Arc::new(self.clone()));
        TransactionProcessor::call_function(
            &*self.template_provider,
            &call_runtime,
            template_address,
            function,
            args,
        )
        .map_err(|e| RuntimeError::CrossTemplateCallFunctionError {
            template_address: *template_address,
            function: function.to_string(),
            details: e.to_string(),
        })
    }

    fn check_resource_auth_hook(&self, hook: &AuthHook) -> Result<(), RuntimeError> {
        let template_address = self
            .tracker
            .write_with(|state| state.get_template_for_component(&hook.component_address))?;
        let template = self.get_template_def(&template_address)?;
        let func = template
            .get_function(&hook.method)
            .ok_or(RuntimeError::InvalidArgument {
                argument: "CreateResourceArg",
                reason: format!("Authorize hook '{}' not found", hook),
            })?;

        if func.is_mut {
            return Err(RuntimeError::InvalidArgument {
                argument: "CreateResourceArg",
                reason: format!("Authorize hook '{}' cannot be mutable", hook),
            });
        }
        if !matches!(func.output, Type::Unit) {
            return Err(RuntimeError::InvalidArgument {
                argument: "CreateResourceArg",
                reason: format!("Authorize hook '{}' must return unit", hook),
            });
        }

        if func.arguments.len() != 3 {
            return Err(RuntimeError::InvalidArgument {
                argument: "CreateResourceArg",
                reason: format!(
                    "Authorize hook '{}' must take 3 arguments (incl &self), but found {}",
                    hook,
                    func.arguments.len()
                ),
            });
        }

        if !matches!(func.arguments[1].arg_type.other(), Some("ResourceAuthAction")) {
            return Err(RuntimeError::InvalidArgument {
                argument: "CreateResourceArg",
                reason: format!("Authorize hook '{}' must take a ResourceAuthAction as argument 1", hook),
            });
        }

        if !matches!(func.arguments[2].arg_type.other(), Some("AuthHookCaller")) {
            return Err(RuntimeError::InvalidArgument {
                argument: "CreateResourceArg",
                reason: format!("Authorize hook '{}' must take an AuthHookCaller as argument 2", hook),
            });
        }

        Ok(())
    }
}

impl<TTemplateProvider: TemplateProvider<Template = LoadedTemplate>> RuntimeInterface
    for RuntimeInterfaceImpl<TTemplateProvider>
{
    fn next_entity_id(&self) -> Result<EntityId, RuntimeError> {
        let id = self.entity_id_provider.next_entity_id()?;
        Ok(id)
    }

    fn emit_event(&self, topic: String, payload: Metadata) -> Result<(), RuntimeError> {
        // forbid template users to emit events that can be confused with the ones emitted by the engine
        if Event::topic_has_std_prefix(&topic) {
            return Err(RuntimeError::InvalidEventTopicStdPrefix { topic });
        }

        self.invoke_modules_on_runtime_call("emit_event")?;

        let component_address_option = self.tracker.read_with(|state| {
            Ok::<_, RuntimeError>(
                state
                    .current_call_scope()?
                    .get_current_component_lock()
                    .and_then(|l| l.address().as_component_address()),
            )
        })?;
        let substate_id = component_address_option.map(SubstateId::Component);
        let tx_hash = self.entity_id_provider.transaction_hash();
        let template_address = self.tracker.get_template_address()?;

        self.tracker
            .add_event(Event::new(substate_id, template_address, tx_hash, topic, payload));
        Ok(())
    }

    fn emit_log(&self, level: LogLevel, message: String) -> Result<(), RuntimeError> {
        self.invoke_modules_on_runtime_call("emit_log")?;

        let log_level = match level {
            LogLevel::Error => log::Level::Error,
            LogLevel::Warn => log::Level::Warn,
            LogLevel::Info => log::Level::Info,
            LogLevel::Debug => log::Level::Debug,
        };

        // eprintln!("{}: {}", log_level, message);
        log::log!(target: "tari::dan::engine::runtime", log_level, "{}", message);
        self.tracker.add_log(LogEntry::new(level, message));
        Ok(())
    }

    fn load_component(&self, address: &ComponentAddress) -> Result<ComponentHeader, RuntimeError> {
        self.invoke_modules_on_runtime_call("load_component")?;
        self.tracker.write_with(|state| state.load_component(address).cloned())
    }

    fn lock_component(&self, address: &ComponentAddress, lock_flag: LockFlag) -> Result<LockedSubstate, RuntimeError> {
        self.tracker.lock_substate(&SubstateId::Component(*address), lock_flag)
    }

    fn caller_context_invoke(
        &self,
        action: CallerContextAction,
        args: EngineArgs,
    ) -> Result<InvokeResult, RuntimeError> {
        self.invoke_modules_on_runtime_call("caller_context_invoke")?;

        match action {
            CallerContextAction::GetCallerPublicKey => {
                args.assert_no_args("CallerContextAction::GetCallerPublicKey")?;
                let sender_public_key =
                    RistrettoPublicKeyBytes::from_bytes(self.transaction_signer_public_key.as_bytes()).expect(
                        "RistrettoPublicKeyBytes::from_bytes should be infallible when called with RistrettoPublicKey \
                         bytes",
                    );

                Ok(InvokeResult::encode(&sender_public_key)?)
            },
            CallerContextAction::GetComponentAddress => self.tracker.read_with(|state| {
                args.assert_no_args("CallerContextAction::GetComponentAddress")?;
                let call_frame = state.current_call_scope()?;
                let maybe_address = call_frame
                    .get_current_component_lock()
                    .map(|l| l.address().as_component_address().unwrap());
                Ok(InvokeResult::encode(&maybe_address)?)
            }),
            CallerContextAction::AllocateNewComponentAddress => self.tracker.write_with(|state| {
                let public_key_address: Option<RistrettoPublicKeyBytes> = args.assert_one_arg()?;
                let public_key_address = public_key_address
                    .map(|pk| {
                        RistrettoPublicKey::from_canonical_bytes(pk.as_bytes()).map_err(|_| {
                            RuntimeError::InvalidArgument {
                                argument: "public_key_address",
                                reason: "Invalid RistrettoPublicKeyBytes".to_string(),
                            }
                        })
                    })
                    .transpose()?;

                let (template, _) = state.current_template()?;
                let address = state
                    .id_provider()?
                    .new_component_address(*template, public_key_address)?;
                let allocation = state.new_address_allocation(address)?;
                Ok(InvokeResult::encode(&allocation)?)
            }),
        }
    }

    fn get_substate(&self, lock: &LockedSubstate) -> Result<SubstateValue, RuntimeError> {
        self.tracker.read_with(|state| {
            let (_, substate) = state.store().get_locked_substate(lock.lock_id())?;
            Ok(substate.clone())
        })
    }

    #[allow(clippy::too_many_lines)]
    fn component_invoke(
        &self,
        component_ref: ComponentRef,
        action: ComponentAction,
        args: EngineArgs,
    ) -> Result<InvokeResult, RuntimeError> {
        self.invoke_modules_on_runtime_call("component_invoke")?;

        debug!(
            target: LOG_TARGET,
            "Component invoke: {} {:?}",
            component_ref,
            action,
        );

        match action {
            ComponentAction::Create => {
                let CreateComponentArg {
                    encoded_state,
                    owner_rule,
                    access_rules,
                    address_allocation,
                } = args.assert_one_arg()?;

                let template_addr = self.tracker.get_template_address()?;
                let template_def = self.get_template_def(&template_addr)?;
                validate_component_access_rule_methods(&access_rules, &template_def)?;

                let owner_key = match owner_rule {
                    OwnerRule::OwnedBySigner => {
                        Some(to_ristretto_public_key_bytes(&self.transaction_signer_public_key))
                    },
                    OwnerRule::None => None,
                    OwnerRule::ByAccessRule(_) => None,
                    OwnerRule::ByPublicKey(key) => Some(key),
                };

                let component_address = self.tracker.new_component(
                    encoded_state,
                    owner_key,
                    owner_rule,
                    access_rules,
                    address_allocation,
                )?;
                Ok(InvokeResult::encode(&component_address)?)
            },
            ComponentAction::GetState => {
                let component_address =
                    component_ref
                        .as_component_address()
                        .ok_or_else(|| RuntimeError::InvalidArgument {
                            argument: "component_ref",
                            reason: "GetState component action should not define a specific component address"
                                .to_string(),
                        })?;
                args.assert_no_args("ComponentAction::GetState")?;
                self.tracker.write_with(|state| {
                    let is_already_locked = state
                        .current_call_scope()?
                        .get_current_component_lock()
                        .map(|l| *l.address() == component_address)
                        .unwrap_or(false);

                    let component_lock = if is_already_locked {
                        state
                            .current_call_scope()?
                            .get_current_component_lock()
                            .cloned()
                            .ok_or(RuntimeError::NotInComponentContext {
                                action: ComponentAction::GetState.into(),
                            })?
                    } else {
                        state.lock_substate(&SubstateId::Component(component_address), LockFlag::Read)?
                    };

                    // We only allow mutating of the current component.
                    if *component_lock.address() != component_address {
                        return Err(RuntimeError::LockError(LockError::SubstateNotLocked {
                            address: SubstateId::Component(component_address),
                        }));
                    }

                    let component = state.get_component(&component_lock)?;
                    let result = InvokeResult::encode(component.state())?;
                    if !is_already_locked {
                        state.unlock_substate(component_lock)?;
                    }

                    Ok(result)
                })
            },
            ComponentAction::SetState => {
                let component_address =
                    component_ref
                        .as_component_address()
                        .ok_or_else(|| RuntimeError::InvalidArgument {
                            argument: "component_ref",
                            reason: "SetState component action should not define a specific component address"
                                .to_string(),
                        })?;
                let component_state = args.assert_one_arg()?;
                self.tracker.write_with(|state| {
                    let component_lock = state
                        .current_call_scope()?
                        .get_current_component_lock()
                        .cloned()
                        .ok_or(RuntimeError::NotInComponentContext {
                            action: ComponentAction::SetState.into(),
                        })?;

                    // We only allow mutating of the current component. Note this check doesnt actually provide any
                    // security itself, it's just checking the engine call is made correctly. The security comes from
                    // the fact that the engine creates the lock on the currently executing component and that is the
                    // lock we use to gain access.
                    if *component_lock.address() != component_address {
                        return Err(RuntimeError::AccessDeniedSetComponentState {
                            attempted_on: component_address.into(),
                            attempted_by: Box::new(component_lock.address().clone()),
                        });
                    }

                    state.modify_component_with(&component_lock, |component| {
                        if component_state == *component.state() {
                            return false;
                        }
                        component.body.set(component_state);
                        true
                    })?;

                    Ok(InvokeResult::unit())
                })
            },
            ComponentAction::SetAccessRules => {
                let component_address =
                    component_ref
                        .as_component_address()
                        .ok_or_else(|| RuntimeError::InvalidArgument {
                            argument: "component_ref",
                            reason: "SetAccessRules component action requires a component address".to_string(),
                        })?;

                let access_rules: ComponentAccessRules = args.assert_one_arg()?;

                self.tracker.write_with(|state| {
                    let component_lock = state
                        .current_call_scope()?
                        .get_current_component_lock()
                        .cloned()
                        .ok_or(RuntimeError::NotInComponentContext {
                            action: ComponentAction::SetAccessRules.into(),
                        })?;
                    // We only allow mutating of the current component. Note this check doesnt actually provide any
                    // security itself, it's just checking the engine call is made correctly. The security comes from
                    // the fact that the engine creates the lock on the currently executing component and that is the
                    // lock we use to gain access.
                    if *component_lock.address() != component_address {
                        return Err(RuntimeError::LockError(LockError::SubstateNotLocked {
                            address: SubstateId::Component(component_address),
                        }));
                    }
                    let component = state.get_component(&component_lock)?;
                    state
                        .authorization()
                        .require_ownership(ComponentAction::SetAccessRules, component.as_ownership())?;

                    state.modify_component_with(&component_lock, |component| {
                        if access_rules == component.access_rules {
                            return false;
                        }
                        component.set_access_rules(access_rules);
                        true
                    })?;

                    Ok::<_, RuntimeError>(())
                })?;

                Ok(InvokeResult::unit())
            },
            ComponentAction::GetTemplateAddress => {
                let component_address =
                    component_ref
                        .as_component_address()
                        .ok_or_else(|| RuntimeError::InvalidArgument {
                            argument: "component_ref",
                            reason: "SetAccessRules component action requires a component address".to_string(),
                        })?;

                args.assert_no_args("Component::GetTemplateAddress")?;

                // The template can never change so we'll just fetch the component
                self.tracker.read_with(|state| {
                    let substate = state.store().get_unmodified_substate(&component_address.into())?;
                    let component = substate
                        .substate_value()
                        .component()
                        .ok_or(RuntimeError::ComponentNotFound {
                            address: component_address,
                        })?;

                    Ok(InvokeResult::encode(&component.template_address)?)
                })
            },
        }
    }

    #[allow(clippy::too_many_lines)]
    fn resource_invoke(
        &self,
        resource_ref: ResourceRef,
        action: ResourceAction,
        args: EngineArgs,
    ) -> Result<InvokeResult, RuntimeError> {
        self.invoke_modules_on_runtime_call("resource_invoke")?;

        debug!(
            target: LOG_TARGET,
            "Resource invoke: {} {:?}",
            resource_ref,
            action,
        );

        match action {
            ResourceAction::Create => {
                let arg: CreateResourceArg = args.assert_one_arg()?;

                if arg
                    .mint_arg
                    .as_ref()
                    .map(|mint| mint.as_resource_type() != arg.resource_type)
                    .unwrap_or(false)
                {
                    return Err(RuntimeError::InvalidArgument {
                        argument: "CreateResourceArg",
                        reason: "Mint argument type does not match resource type".to_string(),
                    });
                }

                if arg.view_key.is_some() && !arg.resource_type.is_confidential() {
                    return Err(RuntimeError::InvalidArgument {
                        argument: "CreateResourceArg",
                        reason: "View key can only be set for confidential resources".to_string(),
                    });
                }

                let owner_key = match &arg.owner_rule {
                    OwnerRule::OwnedBySigner => {
                        Some(to_ristretto_public_key_bytes(&self.transaction_signer_public_key))
                    },
                    OwnerRule::ByPublicKey(key) => Some(*key),
                    OwnerRule::None | OwnerRule::ByAccessRule(_) => None,
                };

                let maybe_view_key = arg
                    .view_key
                    .map(|k| RistrettoPublicKey::from_canonical_bytes(k.as_ref()))
                    .transpose()
                    .map_err(|e| RuntimeError::InvalidArgument {
                        argument: "CreateResourceArg",
                        reason: format!("Invalid view key: {}", e),
                    })?;

                // Check that auth hook is valid
                if let Some(hook) = arg.authorize_hook.as_ref() {
                    self.check_resource_auth_hook(hook)?;
                }

                self.tracker.write_with(|state| {
                    let resource = Resource::new(
                        arg.resource_type,
                        owner_key,
                        arg.owner_rule,
                        arg.access_rules,
                        arg.metadata,
                        maybe_view_key,
                        arg.authorize_hook,
                    );

                    let resource_address = state.id_provider()?.new_resource_address()?;
                    state.new_substate(resource_address, resource)?;
                    let resource_lock =
                        state.lock_substate(&SubstateId::Resource(resource_address), LockFlag::Write)?;

                    let mut output_bucket = None;
                    if let Some(mint_arg) = arg.mint_arg {
                        let bucket_id = state.id_provider()?.new_bucket_id();
                        let container = state.mint_resource(&resource_lock, mint_arg)?;
                        state.new_bucket(bucket_id, container)?;
                        output_bucket = Some(tari_template_lib::models::Bucket::from_id(bucket_id));
                    }

                    state.unlock_substate(resource_lock)?;

                    Ok(InvokeResult::encode(&(resource_address, output_bucket))?)
                })
            },

            ResourceAction::GetTotalSupply => {
                let resource_address =
                    resource_ref
                        .as_resource_address()
                        .ok_or_else(|| RuntimeError::InvalidArgument {
                            argument: "resource_ref",
                            reason: "GetResourceType resource action requires a resource address".to_string(),
                        })?;
                args.assert_no_args("ResourceAction::GetTotalSupply")?;
                self.tracker.write_with(|state| {
                    let locked = state.lock_substate(&SubstateId::Resource(resource_address), LockFlag::Read)?;
                    let resource = state.get_resource(&locked)?;
                    let total_supply = resource.total_supply();
                    state.unlock_substate(locked)?;
                    Ok(InvokeResult::encode(&total_supply)?)
                })
            },
            ResourceAction::GetResourceType => {
                let resource_address =
                    resource_ref
                        .as_resource_address()
                        .ok_or_else(|| RuntimeError::InvalidArgument {
                            argument: "resource_ref",
                            reason: "GetResourceType resource action requires a resource address".to_string(),
                        })?;

                args.assert_no_args("ResourceAction::GetResourceType")?;

                self.tracker.write_with(|state| {
                    let locked = state.lock_substate(&SubstateId::Resource(resource_address), LockFlag::Read)?;
                    let resource = state.get_resource(&locked)?;
                    let resource_type = resource.resource_type();
                    state.unlock_substate(locked)?;
                    Ok(InvokeResult::encode(&resource_type)?)
                })
            },
            ResourceAction::Mint => {
                let resource_address =
                    resource_ref
                        .as_resource_address()
                        .ok_or_else(|| RuntimeError::InvalidArgument {
                            argument: "resource_ref",
                            reason: "Mint resource action requires a resource address".to_string(),
                        })?;
                let mint_resource: MintResourceArg = args.assert_one_arg()?;

                let (resource_lock, maybe_auth_hook, auth_caller) = self.tracker.write_with(|state_mut| {
                    let resource_lock =
                        state_mut.lock_substate(&SubstateId::Resource(resource_address), LockFlag::Write)?;

                    let resource = state_mut.get_resource(&resource_lock)?;

                    state_mut.authorization().check_resource_access_rules(
                        ResourceAuthAction::Mint,
                        resource.as_ownership(),
                        resource.access_rules(),
                    )?;

                    let auth_caller = state_mut.get_auth_caller()?;
                    Ok::<_, RuntimeError>((resource_lock, resource.auth_hook().cloned(), auth_caller))
                })?;

                if let Some(auth_hook) = maybe_auth_hook {
                    self.invoke_resource_access_hook(auth_hook, auth_caller, ResourceAuthAction::Mint)?;
                }

                self.tracker.write_with(|state| {
                    let resource = state.mint_resource(&resource_lock, mint_resource.mint_arg)?;
                    let bucket_id = state.id_provider()?.new_bucket_id();
                    state.new_bucket(bucket_id, resource)?;

                    let bucket = tari_template_lib::models::Bucket::from_id(bucket_id);
                    state.unlock_substate(resource_lock)?;

                    Ok(InvokeResult::encode(&bucket)?)
                })
            },
            ResourceAction::Recall => {
                let resource_address =
                    resource_ref
                        .as_resource_address()
                        .ok_or_else(|| RuntimeError::InvalidArgument {
                            argument: "resource_ref",
                            reason: "Recall resource action requires a resource address".to_string(),
                        })?;
                let arg: RecallResourceArg = args.assert_one_arg()?;

                let (maybe_auth_hook, auth_caller) = self.tracker.write_with(|state_mut| {
                    let resource_lock =
                        state_mut.lock_substate(&SubstateId::Resource(resource_address), LockFlag::Read)?;

                    let resource = state_mut.get_resource(&resource_lock)?;

                    state_mut.authorization().check_resource_access_rules(
                        ResourceAuthAction::Recall,
                        resource.as_ownership(),
                        resource.access_rules(),
                    )?;

                    let auth_hook = resource.auth_hook().cloned();
                    let auth_caller = state_mut.get_auth_caller()?;

                    state_mut.unlock_substate(resource_lock)?;
                    Ok::<_, RuntimeError>((auth_hook, auth_caller))
                })?;

                if let Some(auth_hook) = maybe_auth_hook {
                    self.invoke_resource_access_hook(auth_hook, auth_caller, ResourceAuthAction::Recall)?;
                }

                self.tracker.write_with(|state| {
                    let vault_lock = state.lock_substate(&arg.vault_id.into(), LockFlag::Write)?;

                    let resource = state.recall_resource_from_vault(&vault_lock, arg.resource)?;

                    let bucket_id = state.id_provider()?.new_bucket_id();
                    state.new_bucket(bucket_id, resource)?;

                    state.unlock_substate(vault_lock)?;

                    Ok(InvokeResult::encode(&tari_template_lib::models::Bucket::from_id(
                        bucket_id,
                    ))?)
                })
            },
            ResourceAction::GetNonFungible => {
                let resource_address =
                    resource_ref
                        .as_resource_address()
                        .ok_or_else(|| RuntimeError::InvalidArgument {
                            argument: "resource_ref",
                            reason: "GetNonFungible resource action requires a resource address".to_string(),
                        })?;
                let arg: ResourceGetNonFungibleArg = args.assert_one_arg()?;

                self.tracker.write_with(|state| {
                    let addr = SubstateId::NonFungible(NonFungibleAddress::new(resource_address, arg.id.clone()));
                    let nft_lock = state.lock_substate(&addr, LockFlag::Read)?;

                    let nf_container = state.get_non_fungible(&nft_lock)?;

                    if nf_container.is_burnt() {
                        return Err(RuntimeError::InvalidOpNonFungibleBurnt {
                            op: "GetNonFungible",
                            nf_id: arg.id,
                            resource_address,
                        });
                    }

                    state.unlock_substate(nft_lock)?;

                    Ok(InvokeResult::encode(addr.as_non_fungible_address().unwrap())?)
                })
            },
            ResourceAction::UpdateNonFungibleData => {
                let resource_address =
                    resource_ref
                        .as_resource_address()
                        .ok_or_else(|| RuntimeError::InvalidArgument {
                            argument: "resource_ref",
                            reason: "UpdateNonFungibleData resource action requires a resource address".to_string(),
                        })?;
                let arg: ResourceUpdateNonFungibleDataArg = args.assert_one_arg()?;

                let (maybe_auth_hook, auth_caller) = self.tracker.write_with(|state_mut| {
                    let resource_lock =
                        state_mut.lock_substate(&SubstateId::Resource(resource_address), LockFlag::Read)?;

                    let resource = state_mut.get_resource(&resource_lock)?;

                    state_mut.authorization().check_resource_access_rules(
                        ResourceAuthAction::UpdateNonFungibleData,
                        resource.as_ownership(),
                        resource.access_rules(),
                    )?;

                    let auth_hook = resource.auth_hook().cloned();
                    let auth_caller = state_mut.get_auth_caller()?;

                    state_mut.unlock_substate(resource_lock)?;
                    Ok::<_, RuntimeError>((auth_hook, auth_caller))
                })?;

                if let Some(auth_hook) = maybe_auth_hook {
                    self.invoke_resource_access_hook(
                        auth_hook,
                        auth_caller,
                        ResourceAuthAction::UpdateNonFungibleData,
                    )?;
                }

                self.tracker.write_with(|state| {
                    let addr = NonFungibleAddress::new(resource_address, arg.id);
                    let locked = state.lock_substate(&SubstateId::NonFungible(addr.clone()), LockFlag::Write)?;

                    let nft = state.get_non_fungible_mut(&locked)?;

                    let contents = nft
                        .contents_mut()
                        .ok_or_else(|| RuntimeError::InvalidOpNonFungibleBurnt {
                            op: "UpdateNonFungibleData",
                            resource_address,
                            nf_id: addr.id().clone(),
                        })?;
                    contents.set_mutable_data(arg.data);

                    state.unlock_substate(locked)?;

                    Ok(InvokeResult::unit())
                })
            },
            ResourceAction::UpdateAccessRules => {
                let resource_address =
                    resource_ref
                        .as_resource_address()
                        .ok_or_else(|| RuntimeError::InvalidArgument {
                            argument: "resource_ref",
                            reason: "UpdateAccessRules resource action requires a resource address".to_string(),
                        })?;
                let access_rules: ResourceAccessRules = args.assert_one_arg()?;

                let (resource_lock, maybe_auth_hook, auth_caller) = self.tracker.write_with(|state_mut| {
                    let resource_lock =
                        state_mut.lock_substate(&SubstateId::Resource(resource_address), LockFlag::Write)?;

                    let resource = state_mut.get_resource(&resource_lock)?;

                    state_mut
                        .authorization()
                        .require_ownership(ResourceAuthAction::UpdateAccessRules, resource.as_ownership())?;

                    let auth_caller = state_mut.get_auth_caller()?;
                    Ok::<_, RuntimeError>((resource_lock, resource.auth_hook().cloned(), auth_caller))
                })?;

                if let Some(auth_hook) = maybe_auth_hook {
                    self.invoke_resource_access_hook(auth_hook, auth_caller, ResourceAuthAction::UpdateAccessRules)?;
                }

                self.tracker.write_with(|state| {
                    let resource_mut = state.get_resource_mut(&resource_lock)?;
                    resource_mut.set_access_rules(access_rules);
                    state.unlock_substate(resource_lock)?;

                    Ok(InvokeResult::unit())
                })
            },
        }
    }

    #[allow(clippy::too_many_lines)]
    fn vault_invoke(
        &self,
        vault_ref: VaultRef,
        action: VaultAction,
        args: EngineArgs,
    ) -> Result<InvokeResult, RuntimeError> {
        self.invoke_modules_on_runtime_call("vault_invoke")?;

        debug!(target: LOG_TARGET, "Vault invoke: {} {:?}", vault_ref, action,);

        // Check vault ownership if referencing an ID
        if action.requires_write_access() {
            if let Some(vault_id) = vault_ref.vault_id() {
                self.tracker
                    .read_with(|state| state.check_component_scope(&vault_id.into(), action))?;
            }
        }

        match action {
            VaultAction::Create => {
                let resource_address = vault_ref
                    .resource_address()
                    .ok_or_else(|| RuntimeError::InvalidArgument {
                        argument: "vault_ref",
                        reason: "Create vault action requires a resource address".to_string(),
                    })?;
                args.assert_no_args("CreateVault")?;

                self.tracker.write_with(|state| {
                    let resource_substate_id = SubstateId::Resource(*resource_address);
                    let resource_lock = state.lock_substate(&resource_substate_id, LockFlag::Read)?;
                    let resource = state.get_resource(&resource_lock)?;

                    // Require deposit permissions on the resource to create the vault (even if empty)
                    state.authorization().check_resource_access_rules(
                        ResourceAuthAction::Deposit,
                        resource.as_ownership(),
                        resource.access_rules(),
                    )?;

                    let resource_type = state.get_resource(&resource_lock)?.resource_type();
                    let vault_id = state.id_provider()?.new_vault_id()?;
                    let resource = match resource_type {
                        ResourceType::Fungible => ResourceContainer::fungible(*resource_address, 0.into()),
                        ResourceType::NonFungible => {
                            ResourceContainer::non_fungible(*resource_address, Default::default())
                        },
                        ResourceType::Confidential => {
                            ResourceContainer::confidential(*resource_address, None, Amount::zero())
                        },
                    };

                    let vault = Vault::new(resource);

                    state.new_substate(vault_id, vault)?;
                    debug!(
                        target: LOG_TARGET,
                        "Created vault {} for resource {}",
                        vault_id,
                        resource_address
                    );
                    state.unlock_substate(resource_lock)?;

                    // The resource is not orphaned because of the new vault.
                    state
                        .current_call_scope_mut()?
                        .move_node_to_owned(&resource_substate_id)?;

                    Ok(InvokeResult::encode(&vault_id)?)
                })
            },
            VaultAction::Deposit => {
                let vault_id = vault_ref.vault_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "vault_ref",
                    reason: "Put vault action requires a vault id".to_string(),
                })?;

                let bucket_id: BucketId = args.assert_one_arg()?;

                let (vault_lock, resource_lock, maybe_auth_hook, auth_caller) =
                    self.tracker.write_with(|state_mut| {
                        let vault_lock = state_mut.lock_substate(&SubstateId::Vault(vault_id), LockFlag::Write)?;

                        let resource_address = state_mut.get_vault(&vault_lock)?.resource_address();

                        let resource_lock =
                            state_mut.lock_substate(&SubstateId::Resource(*resource_address), LockFlag::Read)?;

                        let resource = state_mut.get_resource(&resource_lock)?;

                        state_mut.authorization().check_resource_access_rules(
                            ResourceAuthAction::Deposit,
                            resource.as_ownership(),
                            resource.access_rules(),
                        )?;

                        let auth_caller = state_mut.get_auth_caller()?;
                        Ok::<_, RuntimeError>((vault_lock, resource_lock, resource.auth_hook().cloned(), auth_caller))
                    })?;

                if let Some(auth_hook) = maybe_auth_hook {
                    self.invoke_resource_access_hook(auth_hook, auth_caller, ResourceAuthAction::Deposit)?;
                }

                self.tracker.write_with(move |state_mut| {
                    let bucket = state_mut.take_bucket(bucket_id)?;
                    // It is invalid to deposit a bucket that has locked funds
                    if !bucket.locked_amount().is_zero() {
                        return Err(RuntimeError::InvalidOpDepositLockedBucket {
                            bucket_id,
                            locked_amount: bucket.locked_amount(),
                        });
                    }

                    // Emit a builtin event for the deposit
                    self.emit_vault_events(
                        "deposit",
                        vault_id,
                        &vault_lock,
                        bucket.amount(),
                        bucket.resource_type(),
                        state_mut,
                    )?;

                    let vault_mut = state_mut.get_vault_mut(&vault_lock)?;
                    vault_mut.deposit(bucket)?;

                    state_mut.unlock_substate(resource_lock)?;
                    state_mut.unlock_substate(vault_lock)?;

                    Ok(InvokeResult::unit())
                })
            },
            VaultAction::Withdraw => {
                let vault_id = vault_ref.vault_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "vault_ref",
                    reason: "Withdraw vault action requires a vault id".to_string(),
                })?;
                let arg: VaultWithdrawArg = args.assert_one_arg()?;

                let (vault_lock, resource_lock, maybe_auth_hook, auth_caller) =
                    self.tracker.write_with(|state_mut| {
                        let vault_lock = state_mut.lock_substate(&SubstateId::Vault(vault_id), LockFlag::Write)?;

                        let resource_address = state_mut.get_vault(&vault_lock)?.resource_address();

                        let resource_lock =
                            state_mut.lock_substate(&SubstateId::Resource(*resource_address), LockFlag::Read)?;

                        let resource = state_mut.get_resource(&resource_lock)?;

                        state_mut.authorization().check_resource_access_rules(
                            ResourceAuthAction::Withdraw,
                            resource.as_ownership(),
                            resource.access_rules(),
                        )?;

                        let auth_caller = state_mut.get_auth_caller()?;
                        Ok::<_, RuntimeError>((vault_lock, resource_lock, resource.auth_hook().cloned(), auth_caller))
                    })?;

                if let Some(auth_hook) = maybe_auth_hook {
                    self.invoke_resource_access_hook(auth_hook, auth_caller, ResourceAuthAction::Withdraw)?;
                }

                self.tracker.write_with(|state| {
                    let resource = state.get_resource(&resource_lock)?;
                    let maybe_view_key = resource.view_key().cloned();

                    let vault_mut = state.get_vault_mut(&vault_lock)?;
                    let (resource_container, amount) = match arg {
                        VaultWithdrawArg::Fungible { amount } => {
                            let container = vault_mut.withdraw(amount)?;
                            (container, amount)
                        },
                        VaultWithdrawArg::NonFungible { ids } => {
                            let container = vault_mut.withdraw_non_fungibles(&ids)?;
                            let amount =
                                Amount(ids.len().try_into().map_err(|_| RuntimeError::NumericConversionError {
                                    details: "Could not convert to i64".to_owned(),
                                })?);
                            (container, amount)
                        },
                        VaultWithdrawArg::Confidential { proof } => {
                            let amount = proof.revealed_input_amount();
                            let container = vault_mut.withdraw_confidential(*proof, maybe_view_key.as_ref())?;
                            (container, amount)
                        },
                    };

                    // Emit a builtin event for the withdraw
                    self.emit_vault_events(
                        "withdraw",
                        vault_id,
                        &vault_lock,
                        amount,
                        resource_container.resource_type(),
                        state,
                    )?;

                    let bucket_id = state.id_provider()?.new_bucket_id();
                    state.new_bucket(bucket_id, resource_container)?;

                    state.unlock_substate(vault_lock)?;
                    state.unlock_substate(resource_lock)?;

                    let bucket = tari_template_lib::models::Bucket::from_id(bucket_id);
                    Ok(InvokeResult::encode(&bucket)?)
                })
            },
            VaultAction::GetBalance => {
                let vault_id = vault_ref.vault_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "vault_ref",
                    reason: "GetBalance vault action requires a vault id".to_string(),
                })?;
                args.assert_no_args("Vault::GetBalance")?;

                self.tracker.write_with(|state| {
                    let vault_lock = state.lock_substate(&SubstateId::Vault(vault_id), LockFlag::Read)?;
                    let balance = state.get_vault(&vault_lock)?.balance();
                    state.unlock_substate(vault_lock)?;
                    Ok(InvokeResult::encode(&balance)?)
                })
            },
            VaultAction::GetLockedBalance => {
                let vault_id = vault_ref.vault_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "vault_ref",
                    reason: "GetBalance vault action requires a vault id".to_string(),
                })?;
                args.assert_no_args("Vault::GetBalance")?;

                self.tracker.write_with(|state| {
                    let vault_lock = state.lock_substate(&SubstateId::Vault(vault_id), LockFlag::Read)?;
                    let balance = state.get_vault(&vault_lock)?.locked_balance();
                    state.unlock_substate(vault_lock)?;
                    Ok(InvokeResult::encode(&balance)?)
                })
            },
            VaultAction::GetResourceAddress => {
                let vault_id = vault_ref.vault_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "vault_ref",
                    reason: "vault action requires a vault id".to_string(),
                })?;
                args.assert_no_args("Vault::GetResourceAddress")?;

                self.tracker.write_with(|state| {
                    let vault_lock = state.lock_substate(&SubstateId::Vault(vault_id), LockFlag::Read)?;
                    let resource_address = *state.get_vault(&vault_lock)?.resource_address();
                    state.unlock_substate(vault_lock)?;
                    Ok(InvokeResult::encode(&resource_address)?)
                })
            },
            VaultAction::GetNonFungibleIds => {
                let vault_id = vault_ref.vault_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "vault_ref",
                    reason: "vault action requires a vault id".to_string(),
                })?;
                args.assert_no_args("Vault::GetNonFungibleIds")?;

                self.tracker.write_with(|state| {
                    let vault_lock = state.lock_substate(&SubstateId::Vault(vault_id), LockFlag::Read)?;
                    let non_fungible_ids = state.get_vault(&vault_lock)?.get_non_fungible_ids();
                    let result = InvokeResult::encode(&non_fungible_ids)?;
                    state.unlock_substate(vault_lock)?;
                    Ok(result)
                })
            },
            VaultAction::GetCommitmentCount => {
                let vault_id = vault_ref.vault_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "vault_ref",
                    reason: "vault action requires a vault id".to_string(),
                })?;

                args.assert_no_args("Vault::GetCommitmentCount")?;

                self.tracker.write_with(|state| {
                    let vault_lock = state.lock_substate(&SubstateId::Vault(vault_id), LockFlag::Read)?;
                    let commitment_count = state.get_vault(&vault_lock)?.get_commitment_count();
                    state.unlock_substate(vault_lock)?;
                    Ok(InvokeResult::encode(&commitment_count)?)
                })
            },
            VaultAction::ConfidentialReveal => {
                let vault_id = vault_ref.vault_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "vault_ref",
                    reason: "Vault::ConfidentialReveal action requires a vault id".to_string(),
                })?;

                let arg: ConfidentialRevealArg = args.assert_one_arg()?;

                let (vault_lock, resource_lock, maybe_auth_hook, auth_caller) =
                    self.tracker.write_with(|state_mut| {
                        let vault_lock = state_mut.lock_substate(&SubstateId::Vault(vault_id), LockFlag::Write)?;

                        let resource_address = state_mut.get_vault(&vault_lock)?.resource_address();

                        let resource_lock =
                            state_mut.lock_substate(&SubstateId::Resource(*resource_address), LockFlag::Read)?;

                        let resource = state_mut.get_resource(&resource_lock)?;

                        state_mut.authorization().check_resource_access_rules(
                            ResourceAuthAction::Withdraw,
                            resource.as_ownership(),
                            resource.access_rules(),
                        )?;

                        let auth_caller = state_mut.get_auth_caller()?;
                        Ok::<_, RuntimeError>((vault_lock, resource_lock, resource.auth_hook().cloned(), auth_caller))
                    })?;

                if let Some(auth_hook) = maybe_auth_hook {
                    self.invoke_resource_access_hook(auth_hook, auth_caller, ResourceAuthAction::Withdraw)?;
                }

                self.tracker.write_with(|state| {
                    let resource = state.get_resource(&resource_lock)?;
                    let view_key = resource.view_key().cloned();

                    let vault_mut = state.get_vault_mut(&vault_lock)?;
                    let resource_container = vault_mut.reveal_confidential(arg.proof, view_key.as_ref())?;
                    let bucket_id = state.id_provider()?.new_bucket_id();
                    state.new_bucket(bucket_id, resource_container)?;

                    state.unlock_substate(vault_lock)?;
                    state.unlock_substate(resource_lock)?;

                    let bucket = tari_template_lib::models::Bucket::from_id(bucket_id);
                    Ok(InvokeResult::encode(&bucket)?)
                })
            },
            VaultAction::PayFee => {
                let vault_id = vault_ref.vault_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "vault_ref",
                    reason: "PayFee vault action requires a vault id".to_string(),
                })?;

                let arg: PayFeeArg = args.assert_one_arg()?;
                if arg.amount.is_negative() {
                    return Err(RuntimeError::InvalidArgument {
                        argument: "amount",
                        reason: "Amount must be positive".to_string(),
                    });
                }

                self.tracker.write_with(|state| {
                    let vault_lock = state.lock_substate(&SubstateId::Vault(vault_id), LockFlag::Write)?;
                    let resource_address = *state.get_vault(&vault_lock)?.resource_address();
                    if resource_address != XTR {
                        return Err(RuntimeError::InvalidArgument {
                            argument: "vault_ref",
                            reason: format!(
                                "Fees can only be paid using XTR, however the vault contained resource {}",
                                resource_address
                            ),
                        });
                    }
                    let resource_lock = state.lock_substate(&SubstateId::Resource(XTR), LockFlag::Read)?;
                    let resource = state.get_resource(&resource_lock)?;

                    state.authorization().check_resource_access_rules(
                        ResourceAuthAction::Withdraw,
                        resource.as_ownership(),
                        resource.access_rules(),
                    )?;
                    let view_key = resource.view_key().cloned();

                    let vault_mut = state.get_vault_mut(&vault_lock)?;

                    let mut container = ResourceContainer::confidential(XTR, None, Amount::zero());
                    if !arg.amount.is_zero() {
                        let withdrawn = vault_mut.withdraw(arg.amount)?;
                        container.deposit(withdrawn)?;
                    }
                    if let Some(proof) = arg.proof {
                        let revealed = vault_mut.reveal_confidential(proof, view_key.as_ref())?;
                        container.deposit(revealed)?;
                    }
                    if container.amount().is_zero() {
                        return Err(RuntimeError::InvalidArgument {
                            argument: "TakeFeesArg",
                            reason: "Fee payment has zero value".to_string(),
                        });
                    }

                    state.pay_fee(container, vault_id)?;

                    state.unlock_substate(resource_lock)?;
                    state.unlock_substate(vault_lock)?;

                    Ok(InvokeResult::unit())
                })
            },
            VaultAction::CreateProofByResource => {
                let vault_id = vault_ref.vault_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "vault_ref",
                    reason: "CreateProofByResource vault action requires a vault id".to_string(),
                })?;
                args.assert_no_args("CreateProofByResource")?;

                let (vault_lock, resource_lock, maybe_auth_hook, auth_caller) =
                    self.tracker.write_with(|state_mut| {
                        let vault_lock = state_mut.lock_substate(&SubstateId::Vault(vault_id), LockFlag::Write)?;

                        let resource_address = state_mut.get_vault(&vault_lock)?.resource_address();

                        let resource_lock =
                            state_mut.lock_substate(&SubstateId::Resource(*resource_address), LockFlag::Read)?;

                        let resource = state_mut.get_resource(&resource_lock)?;

                        state_mut.authorization().check_resource_access_rules(
                            ResourceAuthAction::Withdraw,
                            resource.as_ownership(),
                            resource.access_rules(),
                        )?;

                        let auth_caller = state_mut.get_auth_caller()?;
                        Ok::<_, RuntimeError>((vault_lock, resource_lock, resource.auth_hook().cloned(), auth_caller))
                    })?;

                if let Some(auth_hook) = maybe_auth_hook {
                    self.invoke_resource_access_hook(auth_hook, auth_caller, ResourceAuthAction::Withdraw)?;
                }

                self.tracker.write_with(|state| {
                    let proof_id = state.id_provider()?.new_proof_id();
                    let vault_mut = state.get_vault_mut(&vault_lock)?;
                    let locked_funds = vault_mut.lock_all(vault_id)?;
                    state.new_proof(proof_id, locked_funds)?;

                    state.unlock_substate(vault_lock)?;
                    state.unlock_substate(resource_lock)?;

                    Ok(InvokeResult::encode(&proof_id)?)
                })
            },
            VaultAction::CreateProofByFungibleAmount => {
                let vault_id = vault_ref.vault_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "vault_ref",
                    reason: "CreateProofByFungibleAmount vault action requires a vault id".to_string(),
                })?;
                let arg: VaultCreateProofByFungibleAmountArg = args.assert_one_arg()?;

                let (vault_lock, resource_lock, maybe_auth_hook, auth_caller) =
                    self.tracker.write_with(|state_mut| {
                        let vault_lock = state_mut.lock_substate(&SubstateId::Vault(vault_id), LockFlag::Write)?;

                        let resource_address = state_mut.get_vault(&vault_lock)?.resource_address();

                        let resource_lock =
                            state_mut.lock_substate(&SubstateId::Resource(*resource_address), LockFlag::Read)?;

                        let resource = state_mut.get_resource(&resource_lock)?;

                        state_mut.authorization().check_resource_access_rules(
                            ResourceAuthAction::Withdraw,
                            resource.as_ownership(),
                            resource.access_rules(),
                        )?;

                        let auth_caller = state_mut.get_auth_caller()?;
                        Ok::<_, RuntimeError>((vault_lock, resource_lock, resource.auth_hook().cloned(), auth_caller))
                    })?;

                if let Some(auth_hook) = maybe_auth_hook {
                    self.invoke_resource_access_hook(auth_hook, auth_caller, ResourceAuthAction::Withdraw)?;
                }

                self.tracker.write_with(|state| {
                    let proof_id = state.id_provider()?.new_proof_id();
                    let vault_mut = state.get_vault_mut(&vault_lock)?;
                    let locked_funds = vault_mut.lock_by_amount(vault_id, arg.amount)?;
                    state.new_proof(proof_id, locked_funds)?;

                    state.unlock_substate(vault_lock)?;
                    state.unlock_substate(resource_lock)?;

                    Ok(InvokeResult::encode(&proof_id)?)
                })
            },
            VaultAction::CreateProofByNonFungibles => {
                let vault_id = vault_ref.vault_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "vault_ref",
                    reason: "CreateProofByNonFungibles vault action requires a vault id".to_string(),
                })?;
                let arg: VaultCreateProofByNonFungiblesArg = args.assert_one_arg()?;

                let (vault_lock, resource_lock, maybe_auth_hook, auth_caller) =
                    self.tracker.write_with(|state_mut| {
                        let vault_lock = state_mut.lock_substate(&SubstateId::Vault(vault_id), LockFlag::Write)?;

                        let resource_address = state_mut.get_vault(&vault_lock)?.resource_address();

                        let resource_lock =
                            state_mut.lock_substate(&SubstateId::Resource(*resource_address), LockFlag::Read)?;

                        let resource = state_mut.get_resource(&resource_lock)?;

                        state_mut.authorization().check_resource_access_rules(
                            ResourceAuthAction::Withdraw,
                            resource.as_ownership(),
                            resource.access_rules(),
                        )?;

                        let auth_caller = state_mut.get_auth_caller()?;
                        Ok::<_, RuntimeError>((vault_lock, resource_lock, resource.auth_hook().cloned(), auth_caller))
                    })?;

                if let Some(auth_hook) = maybe_auth_hook {
                    self.invoke_resource_access_hook(auth_hook, auth_caller, ResourceAuthAction::Withdraw)?;
                }

                self.tracker.write_with(|state| {
                    let proof_id = state.id_provider()?.new_proof_id();
                    let vault_mut = state.get_vault_mut(&vault_lock)?;
                    let locked_funds = vault_mut.lock_by_non_fungible_ids(vault_id, arg.ids)?;
                    state.new_proof(proof_id, locked_funds)?;

                    state.unlock_substate(vault_lock)?;
                    state.unlock_substate(resource_lock)?;

                    Ok(InvokeResult::encode(&proof_id)?)
                })
            },
            VaultAction::CreateProofByConfidentialResource => todo!("CreateProofByConfidentialResource"),
            VaultAction::GetNonFungibles => {
                let vault_id = vault_ref.vault_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "vault_ref",
                    reason: "GetNonFungibles vault action requires a vault id".to_string(),
                })?;
                args.assert_no_args("Vault::GetNonFungibles")?;

                self.tracker.write_with(|state| {
                    let vault_lock = state.lock_substate(&SubstateId::Vault(vault_id), LockFlag::Read)?;
                    let resource_address = state.get_vault(&vault_lock)?.resource_address();
                    let nft_ids = state.get_vault(&vault_lock)?.get_non_fungible_ids();
                    let nfts: Vec<NonFungible> = nft_ids
                        .iter()
                        .map(|id| NonFungibleAddress::new(*resource_address, id.clone()))
                        .map(NonFungible::new)
                        .collect();

                    let result = InvokeResult::encode(&nfts)?;
                    state.unlock_substate(vault_lock)?;
                    Ok(result)
                })
            },
        }
    }

    #[allow(clippy::too_many_lines)]
    fn bucket_invoke(
        &self,
        bucket_ref: BucketRef,
        action: BucketAction,
        args: EngineArgs,
    ) -> Result<InvokeResult, RuntimeError> {
        self.invoke_modules_on_runtime_call("bucket_invoke")?;

        debug!(target: LOG_TARGET, "Bucket invoke: {} {:?}", bucket_ref, action,);

        match action {
            BucketAction::GetResourceAddress => {
                let bucket_id = bucket_ref.bucket_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "bucket_ref",
                    reason: "GetResourceAddress action requires a bucket id".to_string(),
                })?;
                args.assert_no_args("Bucket::GetResourceAddress")?;

                self.tracker.read_with(|state| {
                    let bucket = state.get_bucket(bucket_id)?;
                    Ok(InvokeResult::encode(bucket.resource_address())?)
                })
            },
            BucketAction::GetResourceType => {
                let bucket_id = bucket_ref.bucket_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "bucket_ref",
                    reason: "GetResourceType action requires a bucket id".to_string(),
                })?;
                args.assert_no_args("Bucket::GetResourceType")?;

                self.tracker.read_with(|state| {
                    let bucket = state.get_bucket(bucket_id)?;
                    Ok(InvokeResult::encode(&bucket.resource_type())?)
                })
            },
            BucketAction::GetAmount => {
                let bucket_id = bucket_ref.bucket_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "bucket_ref",
                    reason: "GetAmount bucket action requires a bucket id".to_string(),
                })?;

                args.assert_no_args("Bucket::GetAmount")?;
                self.tracker.read_with(|state| {
                    let bucket = state.get_bucket(bucket_id)?;
                    Ok(InvokeResult::encode(&bucket.amount())?)
                })
            },
            BucketAction::Take => {
                let bucket_id = bucket_ref.bucket_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "bucket_ref",
                    reason: "Take bucket action requires a bucket id".to_string(),
                })?;
                let amount = args.assert_one_arg()?;

                self.tracker.write_with(|state| {
                    let bucket = state.get_bucket_mut(bucket_id)?;
                    let resource = bucket.take(amount)?;
                    let bucket_id = state.id_provider()?.new_bucket_id();
                    state.new_bucket(bucket_id, resource)?;
                    Ok(InvokeResult::encode(&bucket_id)?)
                })
            },
            BucketAction::TakeConfidential => {
                let bucket_id = bucket_ref.bucket_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "bucket_ref",
                    reason: "Take bucket action requires a bucket id".to_string(),
                })?;
                let proof = args.assert_one_arg()?;

                self.tracker.write_with(|state| {
                    let bucket = state.get_bucket(bucket_id)?;
                    let resource_lock = state.lock_substate(&(*bucket.resource_address()).into(), LockFlag::Read)?;
                    let resource = state.get_resource(&resource_lock)?;
                    let view_key = resource.view_key().cloned();
                    let bucket_mut = state.get_bucket_mut(bucket_id)?;
                    let resource = bucket_mut.take_confidential(proof, view_key.as_ref())?;
                    let bucket_id = state.id_provider()?.new_bucket_id();
                    state.new_bucket(bucket_id, resource)?;
                    state.unlock_substate(resource_lock)?;
                    Ok(InvokeResult::encode(&bucket_id)?)
                })
            },
            BucketAction::Join => {
                let bucket_id = bucket_ref.bucket_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "bucket_ref",
                    reason: "Join bucket action requires a bucket id".to_string(),
                })?;
                let other_bucket_id = args.assert_one_arg()?;

                self.tracker.write_with(|state| {
                    let other_bucket = state.take_bucket(other_bucket_id)?;
                    let bucket = state.get_bucket_mut(bucket_id)?;
                    bucket.join(other_bucket)?;
                    Ok(InvokeResult::encode(&bucket_id)?)
                })
            },
            BucketAction::RevealConfidential => {
                let bucket_id = bucket_ref.bucket_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "bucket_ref",
                    reason: "RevealConfidential bucket action requires a bucket id".to_string(),
                })?;
                let proof = args.assert_one_arg()?;
                self.tracker.write_with(|state| {
                    let bucket = state.get_bucket(bucket_id)?;
                    let resource_lock = state.lock_substate(&(*bucket.resource_address()).into(), LockFlag::Read)?;
                    let resource = state.get_resource(&resource_lock)?;
                    let view_key = resource.view_key().cloned();
                    let bucket = state.get_bucket_mut(bucket_id)?;
                    let resource = bucket.reveal_confidential(proof, view_key.as_ref())?;
                    let bucket_id = state.id_provider()?.new_bucket_id();
                    state.new_bucket(bucket_id, resource)?;
                    state.unlock_substate(resource_lock)?;
                    Ok(InvokeResult::encode(&bucket_id)?)
                })
            },
            BucketAction::Burn => {
                let bucket_id = bucket_ref.bucket_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "bucket_ref",
                    reason: "Burn bucket action requires a bucket id".to_string(),
                })?;

                let (resource_lock, maybe_auth_hook, auth_caller) = self.tracker.write_with(|state_mut| {
                    let bucket = state_mut.get_bucket(bucket_id)?;

                    let resource_lock =
                        state_mut.lock_substate(&SubstateId::Resource(*bucket.resource_address()), LockFlag::Write)?;

                    let resource = state_mut.get_resource(&resource_lock)?;

                    state_mut.authorization().check_resource_access_rules(
                        ResourceAuthAction::Burn,
                        resource.as_ownership(),
                        resource.access_rules(),
                    )?;

                    let auth_caller = state_mut.get_auth_caller()?;
                    Ok::<_, RuntimeError>((resource_lock, resource.auth_hook().cloned(), auth_caller))
                })?;

                if let Some(auth_hook) = maybe_auth_hook {
                    self.invoke_resource_access_hook(auth_hook, auth_caller, ResourceAuthAction::Burn)?;
                }

                self.tracker.write_with(|state| {
                    let bucket = state.take_bucket(bucket_id)?;
                    let burnt_amount = bucket.amount();
                    state.burn_bucket(bucket)?;

                    let resource_mut = state.get_resource_mut(&resource_lock)?;
                    resource_mut.decrease_total_supply(burnt_amount);

                    state.unlock_substate(resource_lock)?;

                    Ok(InvokeResult::unit())
                })
            },
            BucketAction::CreateProof => {
                let bucket_id = bucket_ref.bucket_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "bucket_ref",
                    reason: "CreateProof bucket action requires a bucket id".to_string(),
                })?;

                args.assert_no_args("Bucket::CreateProof")?;

                let (maybe_auth_hook, auth_caller) = self.tracker.write_with(|state_mut| {
                    let bucket = state_mut.get_bucket(bucket_id)?;

                    let resource_lock =
                        state_mut.lock_substate(&SubstateId::Resource(*bucket.resource_address()), LockFlag::Read)?;

                    let resource = state_mut.get_resource(&resource_lock)?;

                    state_mut.authorization().check_resource_access_rules(
                        ResourceAuthAction::Withdraw,
                        resource.as_ownership(),
                        resource.access_rules(),
                    )?;

                    let auth_hook = resource.auth_hook().cloned();
                    let auth_caller = state_mut.get_auth_caller()?;

                    state_mut.unlock_substate(resource_lock)?;
                    Ok::<_, RuntimeError>((auth_hook, auth_caller))
                })?;

                if let Some(auth_hook) = maybe_auth_hook {
                    self.invoke_resource_access_hook(auth_hook, auth_caller, ResourceAuthAction::Withdraw)?;
                }

                self.tracker.write_with(|state| {
                    let locked_funds = state.get_bucket_mut(bucket_id)?.lock_all()?;

                    let proof_id = state.id_provider()?.new_proof_id();
                    state.new_proof(proof_id, locked_funds)?;

                    Ok(InvokeResult::encode(&proof_id)?)
                })
            },
            BucketAction::GetNonFungibleIds => {
                let bucket_id = bucket_ref.bucket_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "bucket_ref",
                    reason: "GetNonFungibleIds bucket action requires a bucket id".to_string(),
                })?;
                args.assert_no_args("Bucket::GetNonFungibleIds")?;

                self.tracker.write_with(|state| {
                    let bucket = state.get_bucket(bucket_id)?;
                    Ok(InvokeResult::encode(bucket.non_fungible_ids())?)
                })
            },
            BucketAction::GetNonFungibles => {
                let bucket_id = bucket_ref.bucket_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "bucket_ref",
                    reason: "GetNonFungibles bucket action requires a bucket id".to_string(),
                })?;
                args.assert_no_args("Bucket::GetNonFungibles")?;

                self.tracker.write_with(|state| {
                    let bucket = state.get_bucket(bucket_id)?;
                    let resource_address = bucket.resource_address();
                    let nft_ids = bucket.non_fungible_ids();
                    let nfts: Vec<NonFungible> = nft_ids
                        .iter()
                        .map(|id| NonFungibleAddress::new(*resource_address, id.clone()))
                        .map(NonFungible::new)
                        .collect();

                    Ok(InvokeResult::encode(&nfts)?)
                })
            },
            BucketAction::CountConfidentialCommitments => {
                let bucket_id = bucket_ref.bucket_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "bucket_ref",
                    reason: "CountConfidentialCommitments bucket action requires a bucket id".to_string(),
                })?;
                args.assert_no_args("Bucket::CountConfidentialCommitments")?;

                self.tracker.write_with(|state| {
                    let bucket = state.get_bucket(bucket_id)?;
                    Ok(InvokeResult::encode(&bucket.number_of_confidential_commitments())?)
                })
            },
        }
    }

    fn proof_invoke(
        &self,
        proof_ref: ProofRef,
        action: ProofAction,
        args: EngineArgs,
    ) -> Result<InvokeResult, RuntimeError> {
        self.invoke_modules_on_runtime_call("proof_invoke")?;

        debug!(
            target: LOG_TARGET,
            "Proof invoke: {} {:?}",
            proof_ref,
            action,
        );

        match action {
            ProofAction::GetAmount => {
                let proof_id = proof_ref.proof_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "proof_ref",
                    reason: "GetAmount proof action requires a proof id".to_string(),
                })?;
                args.assert_no_args("Proof.GetAmount")?;
                self.tracker.write_with(|state| {
                    let proof = state.get_proof(proof_id)?;
                    Ok(InvokeResult::encode(&proof.amount())?)
                })
            },
            ProofAction::GetResourceAddress => {
                let proof_id = proof_ref.proof_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "proof_ref",
                    reason: "GetResourceAddress proof action requires a proof id".to_string(),
                })?;
                args.assert_no_args("Proof.GetResourceAddress")?;
                self.tracker.write_with(|state| {
                    let proof = state.get_proof(proof_id)?;
                    Ok(InvokeResult::encode(proof.resource_address())?)
                })
            },
            ProofAction::GetResourceType => {
                let proof_id = proof_ref.proof_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "proof_ref",
                    reason: "GetResourceType proof action requires a proof id".to_string(),
                })?;

                args.assert_no_args("Proof.GetResourceType")?;

                self.tracker.write_with(|state| {
                    let proof = state.get_proof(proof_id)?;
                    Ok(InvokeResult::encode(&proof.resource_type())?)
                })
            },
            ProofAction::GetNonFungibles => {
                let proof_id = proof_ref.proof_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "proof_ref",
                    reason: "GetNonFungibles proof action requires a proof id".to_string(),
                })?;

                args.assert_no_args("Proof.GetNonFungibles")?;

                self.tracker.write_with(|state| {
                    let proof = state.get_proof(proof_id)?;
                    let nfts = proof.non_fungible_token_ids();
                    Ok(InvokeResult::encode(&nfts)?)
                })
            },
            ProofAction::Authorize => {
                let proof_id = proof_ref.proof_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "proof_ref",
                    reason: "Authorize proof action requires a proof id".to_string(),
                })?;
                args.assert_no_args("Proof.CreateAccess")?;

                self.tracker.write_with(|state| {
                    if !state.proof_exists(proof_id) {
                        return Ok(InvokeResult::encode(&Err::<(), _>(NotAuthorized))?);
                    }
                    state.current_call_scope_mut()?.auth_scope_mut().add_proof(proof_id);
                    Ok(InvokeResult::encode(&Ok::<_, NotAuthorized>(()))?)
                })
            },
            ProofAction::DropAuthorize => {
                let proof_id = proof_ref.proof_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "proof_ref",
                    reason: "DropAuthorize proof action requires a proof id".to_string(),
                })?;
                args.assert_no_args("Proof.DropAuthorize")?;

                self.tracker.write_with(|state| {
                    if !state.proof_exists(proof_id) {
                        return Err(RuntimeError::ProofNotFound { proof_id });
                    }
                    state.current_call_scope_mut()?.auth_scope_mut().remove_proof(&proof_id);

                    Ok(InvokeResult::unit())
                })
            },
            ProofAction::Drop => {
                let proof_id = proof_ref.proof_id().ok_or_else(|| RuntimeError::InvalidArgument {
                    argument: "proof_ref",
                    reason: "Drop proof action requires a proof id".to_string(),
                })?;
                args.assert_no_args("Proof.Drop")?;

                self.tracker.write_with(|state| state.drop_proof(proof_id))?;

                Ok(InvokeResult::unit())
            },
        }
    }

    fn workspace_invoke(&self, action: WorkspaceAction, args: EngineArgs) -> Result<InvokeResult, RuntimeError> {
        self.invoke_modules_on_runtime_call("workspace_invoke")?;

        debug!(target: LOG_TARGET, "Workspace invoke: {:?}", action,);

        match action {
            WorkspaceAction::ListBuckets => {
                let bucket_ids = self.tracker.list_buckets();
                Ok(InvokeResult::encode(&bucket_ids)?)
            },
            // Basically names an output on the workspace so that you can refer to it as an
            // Arg::Variable
            WorkspaceAction::PutLastInstructionOutput => {
                let key = args.get(0)?;
                let last_output = self
                    .tracker
                    .take_last_instruction_output()
                    .ok_or(RuntimeError::NoLastInstructionOutput)?;

                self.validate_return_value(&last_output)?;

                self.tracker
                    .with_workspace_mut(|workspace| workspace.insert(key, last_output))?;
                Ok(InvokeResult::unit())
            },
            WorkspaceAction::Get => {
                let key: Vec<u8> = args.get(0)?;
                let value = self.tracker.get_from_workspace(&key)?;
                Ok(InvokeResult::from_value(value.into_value()))
            },

            WorkspaceAction::DropAllProofs => {
                let proofs = self
                    .tracker
                    .with_workspace_mut(|workspace| workspace.drain_all_proofs());

                self.tracker.write_with(|state| {
                    for proof_id in proofs {
                        state.drop_proof(proof_id)?;
                    }
                    Ok(InvokeResult::unit())
                })
            },
            WorkspaceAction::AssertBucketContains => {
                let key: Vec<u8> = args.get(0)?;
                let resource_address: ResourceAddress = args.get(1)?;
                let min_amount: Amount = args.get(2)?;

                // get the bucket from the workspace
                let value = self.tracker.get_from_workspace(&key)?;
                let bucket_id = value
                    .bucket_ids()
                    .first()
                    .ok_or_else(|| RuntimeError::AssertError(AssertError::InvalidBucket))?;

                self.tracker.read_with(|state| {
                    let bucket = state.get_bucket(*bucket_id)?;

                    // validate the bucket resource
                    if *bucket.resource_address() != resource_address {
                        return Err(RuntimeError::AssertError(AssertError::InvalidResource {
                            expected: resource_address,
                            got: *bucket.resource_address(),
                        }));
                    }

                    // validate the bucket amount
                    if bucket.amount() < min_amount {
                        return Err(RuntimeError::AssertError(AssertError::InvalidAmount {
                            expected: min_amount,
                            got: bucket.amount(),
                        }));
                    }

                    Ok(InvokeResult::unit())
                })
            },
        }
    }

    fn non_fungible_invoke(
        &self,
        nf_addr: NonFungibleAddress,
        action: NonFungibleAction,
        args: EngineArgs,
    ) -> Result<InvokeResult, RuntimeError> {
        self.invoke_modules_on_runtime_call("non_fungible_invoke")?;
        debug!(
            target: LOG_TARGET,
            "NonFungible invoke: {} {:?}",
            nf_addr,
            action,
        );

        match action {
            NonFungibleAction::GetData => {
                args.assert_no_args("NonFungibleAction::GetData")?;
                self.tracker.write_with(|state| {
                    let nft_lock = state.lock_substate(&SubstateId::NonFungible(nf_addr.clone()), LockFlag::Read)?;
                    let nft = state.get_non_fungible(&nft_lock)?;
                    let contents = nft
                        .contents()
                        .ok_or_else(|| RuntimeError::InvalidOpNonFungibleBurnt {
                            op: "GetData",
                            resource_address: *nf_addr.resource_address(),
                            nf_id: nf_addr.id().clone(),
                        })?
                        .data()
                        .clone();
                    state.unlock_substate(nft_lock)?;
                    Ok(InvokeResult::from_value(contents))
                })
            },
            NonFungibleAction::GetMutableData => {
                args.assert_no_args("NonFungibleAction::GetMutableData")?;

                self.tracker.write_with(|state| {
                    let nft_lock = state.lock_substate(&SubstateId::NonFungible(nf_addr.clone()), LockFlag::Read)?;
                    let nft = state.get_non_fungible(&nft_lock)?;
                    let contents = nft
                        .contents()
                        .ok_or_else(|| RuntimeError::InvalidOpNonFungibleBurnt {
                            op: "GetMutableData",
                            resource_address: *nf_addr.resource_address(),
                            nf_id: nf_addr.id().clone(),
                        })?
                        .mutable_data()
                        .clone();
                    state.unlock_substate(nft_lock)?;

                    Ok(InvokeResult::from_value(contents))
                })
            },
        }
    }

    fn consensus_invoke(&self, action: ConsensusAction) -> Result<InvokeResult, RuntimeError> {
        self.invoke_modules_on_runtime_call("consensus_invoke")?;
        match action {
            ConsensusAction::GetCurrentEpoch => {
                let epoch = self.tracker.get_current_epoch()?;
                Ok(InvokeResult::encode(&epoch)?)
            },
        }
    }

    fn generate_random_invoke(&self, action: GenerateRandomAction) -> Result<InvokeResult, RuntimeError> {
        self.invoke_modules_on_runtime_call("generate_random_invoke")?;
        match action {
            GenerateRandomAction::GetRandomBytes { len } => {
                let random = self.tracker.get_pseudorandom_bytes(len as usize)?;
                Ok(InvokeResult::encode(&random)?)
            },
        }
    }

    fn call_invoke(&self, action: CallAction, args: EngineArgs) -> Result<InvokeResult, RuntimeError> {
        self.invoke_modules_on_runtime_call("call_invoke")?;
        debug!(
            target: LOG_TARGET,
            "Call invoke: {:?} {:?}",
            action,
            args,
        );

        let exec_result = match action {
            CallAction::CallFunction => {
                let CallFunctionArg {
                    template_address,
                    function,
                    args,
                } = args.assert_one_arg()?;

                self.invoke_template_function(&template_address, &function, args)?
            },
            CallAction::CallMethod => {
                let CallMethodArg {
                    component_address,
                    method,
                    args,
                } = args.assert_one_arg()?;

                self.invoke_component_method(&component_address, &method, args)?
            },
        };

        Ok(InvokeResult::from_value(exec_result.indexed.into_value()))
    }

    fn generate_uuid(&self) -> Result<[u8; 32], RuntimeError> {
        self.invoke_modules_on_runtime_call("generate_uuid")?;
        self.tracker.read_with(|state| {
            let id_provider = state.id_provider()?;
            Ok(id_provider.new_uuid()?)
        })
    }

    fn set_last_instruction_output(&self, value: IndexedValue) -> Result<(), RuntimeError> {
        self.invoke_modules_on_runtime_call("set_last_instruction_output")?;
        self.tracker.write_with(|state| {
            state.set_last_instruction_output(value);
        });
        Ok(())
    }

    fn claim_burn(&self, claim: ConfidentialClaim) -> Result<(), RuntimeError> {
        let ConfidentialClaim {
            public_key: diffie_hellman_public_key,
            output_address,
            range_proof,
            proof_of_knowledge,
            withdraw_proof,
        } = claim;
        // 1. Must exist
        let unclaimed_output = self.tracker.take_unclaimed_confidential_output(output_address)?;
        // 2. owner_sig must be valid
        let message = ownership_proof_hasher64(self.network)
            .chain(proof_of_knowledge.public_nonce())
            .chain(&unclaimed_output.commitment)
            .chain(&self.transaction_signer_public_key)
            .finalize();

        if !proof_of_knowledge.verify_challenge(&unclaimed_output.commitment, &message, get_commitment_factory()) {
            warn!(target: LOG_TARGET, "Claim burn failed - Invalid signature");
            return Err(RuntimeError::InvalidClaimingSignature);
        }

        // 3. range_proof must be valid
        if !get_range_proof_service(1).verify(&range_proof, &unclaimed_output.commitment) {
            warn!(target: LOG_TARGET, "Claim burn failed - Invalid range proof");
            return Err(RuntimeError::InvalidRangeProof);
        }

        // 4. Create the confidential resource
        let mut resource = ResourceContainer::confidential(
            CONFIDENTIAL_TARI_RESOURCE_ADDRESS,
            Some((unclaimed_output.commitment.clone(), ConfidentialOutput {
                commitment: unclaimed_output.commitment,
                stealth_public_nonce: diffie_hellman_public_key,
                encrypted_data: unclaimed_output.encrypted_data,
                minimum_value_promise: 0,
                viewable_balance: None,
            })),
            Amount::zero(),
        );

        // If a withdraw proof is provided, we execute it and deposit back into the resource
        // This allows some funds to be revealed and/or reblinded within a single instruction
        if let Some(proof) = withdraw_proof {
            let withdraw = resource.withdraw_confidential(proof, None)?;
            resource.deposit(withdraw)?;
        }

        self.tracker.write_with(|state| {
            let bucket_id = state.new_bucket_id();
            state.new_bucket(bucket_id, resource)?;
            state.set_last_instruction_output(IndexedValue::from_type(&bucket_id)?);
            Ok::<_, RuntimeError>(())
        })?;

        Ok(())
    }

    fn claim_validator_fees(&self, pool_address: ValidatorFeePoolAddress) -> Result<(), RuntimeError> {
        self.tracker.write_with(|state| {
            let resource = state.withdraw_all_fees_from_pool(pool_address)?;
            let bucket_id = state.new_bucket_id();
            state.new_bucket(bucket_id, resource)?;
            state.set_last_instruction_output(IndexedValue::from_type(&bucket_id)?);
            Ok::<_, RuntimeError>(())
        })?;

        Ok(())
    }

    fn set_fee_checkpoint(&self) -> Result<(), RuntimeError> {
        if self.tracker.total_fee_payments() < self.tracker.total_fee_charges() {
            return Err(RuntimeError::InsufficientFeesPaid {
                required_fee: self.tracker.total_fee_charges(),
                fees_paid: self.tracker.total_fee_payments(),
            });
        }
        self.tracker.fee_checkpoint()
    }

    fn reset_to_fee_checkpoint(&self) -> Result<(), RuntimeError> {
        warn!(target: LOG_TARGET, "Resetting to fee checkpoint");
        self.tracker.reset_to_fee_checkpoint()
    }

    fn finalize(&self) -> Result<FinalizeResult, RuntimeError> {
        self.invoke_modules_on_runtime_call("finalize")?;

        // If the fee module is present, this will add substate storage fees
        self.invoke_modules_on_before_finalize()?;

        if !self.tracker.are_fees_paid_in_full() {
            self.reset_to_fee_checkpoint()?;
        }

        let substates_to_persist = self.tracker.take_substates_to_persist();
        let mut finalized = self.tracker.finalize(substates_to_persist)?;

        if !finalized.fee_receipt.is_paid_in_full() {
            let reason = RejectReason::InsufficientFeesPaid(format!(
                "Required fees {} but {} paid",
                finalized.fee_receipt.total_fees_charged(),
                finalized.fee_receipt.total_fees_paid()
            ));
            finalized.result = if let Some(accept) = finalized.result.accept() {
                TransactionResult::AcceptFeeRejectRest(accept.clone(), reason)
            } else {
                TransactionResult::Reject(reason)
            };
        }

        Ok(finalized)
    }

    fn validate_finalized(&self) -> Result<(), RuntimeError> {
        self.tracker.read_with(|state| {
            state.validate_finalized()?;
            Ok(())
        })
    }

    fn check_component_access_rules(&self, method: &str, locked: &LockedSubstate) -> Result<(), RuntimeError> {
        self.tracker
            .read_with(|state| state.authorization().check_component_access_rules(method, locked))
    }

    fn validate_return_value(&self, value: &IndexedValue) -> Result<(), RuntimeError> {
        self.tracker
            .read_with(|state| state.check_all_substates_known(value.well_known_types()))
    }

    fn push_call_frame(&self, frame: PushCallFrame) -> Result<(), RuntimeError> {
        self.tracker.push_call_frame(frame, self.max_call_depth)?;
        Ok(())
    }

    fn pop_call_frame(&self) -> Result<(), RuntimeError> {
        self.tracker.pop_call_frame()?;
        Ok(())
    }

    fn builtin_template_invoke(&self, action: BuiltinTemplateAction) -> Result<InvokeResult, RuntimeError> {
        self.invoke_modules_on_runtime_call("builtin_template_invoke")?;

        let address = match action {
            BuiltinTemplateAction::GetTemplateAddress { bultin } => match bultin {
                BuiltinTemplate::Account => ACCOUNT_TEMPLATE_ADDRESS,
                BuiltinTemplate::AccountNft => ACCOUNT_NFT_TEMPLATE_ADDRESS,
            },
        };

        Ok(InvokeResult::encode(&address)?)
    }

    fn publish_template(&self, template: Vec<u8>) -> Result<(), RuntimeError> {
        self.invoke_modules_on_runtime_call("publish_template")?;
        self.tracker.write_with(|state| {
            let binary_hash = hash_template_code(&template);
            let template_address = PublishedTemplateAddress::from_author_and_binary_hash(
                &self.transaction_signer_public_key,
                &binary_hash,
            );
            state.new_substate(
                template_address,
                SubstateValue::Template(PublishedTemplate {
                    // We essentially store the pre-image of the template address in the substate
                    binary_hash,
                    author: self.transaction_signer_public_key.clone(),
                }),
            )?;
            // Mark template substate as owned by current call stack
            let scope_mut = state.current_call_scope_mut()?;
            scope_mut.move_node_to_owned(&template_address.into())?;

            Ok(())
        })
    }
}

fn validate_component_access_rule_methods(
    access_rules: &ComponentAccessRules,
    template_def: &TemplateDef,
) -> Result<(), RuntimeError> {
    for (name, _) in access_rules.method_access_rules_iter() {
        if template_def.functions().iter().all(|f| f.name != *name) {
            return Err(RuntimeError::InvalidMethodAccessRule {
                template_name: template_def.template_name().to_string(),
                details: format!("No method '{}' found in template", name),
            });
        }
    }
    Ok(())
}
