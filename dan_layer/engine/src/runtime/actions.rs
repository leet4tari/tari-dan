//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause

use std::fmt::{Display, Formatter};

use tari_template_lib::{
    args::{ComponentAction, VaultAction},
    auth::ResourceAuthAction,
    models::ComponentAddress,
};

#[derive(Debug, Clone)]
pub enum ActionIdent {
    Native(NativeAction),
    ComponentCallMethod {
        component_address: ComponentAddress,
        method: String,
    },
}

impl From<NativeAction> for ActionIdent {
    fn from(native: NativeAction) -> Self {
        Self::Native(native)
    }
}

impl From<ComponentAction> for ActionIdent {
    fn from(component_action: ComponentAction) -> Self {
        Self::Native(NativeAction::Component(component_action))
    }
}

impl From<ResourceAuthAction> for ActionIdent {
    fn from(action: ResourceAuthAction) -> Self {
        Self::Native(NativeAction::Resource(action))
    }
}

impl From<VaultAction> for ActionIdent {
    fn from(action: VaultAction) -> Self {
        Self::Native(NativeAction::Vault(action))
    }
}

impl Display for ActionIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ActionIdent::Native(native_fn) => write!(f, "native.{}", native_fn),
            ActionIdent::ComponentCallMethod {
                component_address,
                method,
            } => {
                write!(f, "call component method '{method}' on {component_address}")
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum NativeAction {
    WithdrawValidatorFunds,
    Component(ComponentAction),
    Resource(ResourceAuthAction),
    Vault(VaultAction),
}

impl Display for NativeAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NativeAction::WithdrawValidatorFunds => write!(f, "withdraw_validator_funds"),
            NativeAction::Component(action) => write!(f, "component.call_method.{:?}", action),
            NativeAction::Resource(action) => write!(f, "resource.{:?}", action),
            NativeAction::Vault(action) => write!(f, "vault.{:?}", action),
        }
    }
}
