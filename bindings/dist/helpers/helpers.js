//   Copyright 2023 The Tari Project
//   SPDX-License-Identifier: BSD-3-Clause
export function substateIdToString(substateId) {
    if (substateId === null || substateId === undefined) {
        return "";
    }
    if (typeof substateId === "string") {
        return substateId;
    }
    if (typeof substateId !== "object") {
        throw new Error(`Cannot convert: ${JSON.stringify(substateId)} to string`);
    }
    if ("Component" in substateId) {
        return substateId.Component;
    }
    if ("Resource" in substateId) {
        return substateId.Resource;
    }
    if ("Vault" in substateId) {
        return substateId.Vault;
    }
    if ("UnclaimedConfidentialOutput" in substateId) {
        return substateId.UnclaimedConfidentialOutput;
    }
    if ("NonFungible" in substateId) {
        return substateId.NonFungible;
    }
    if ("NonFungibleIndex" in substateId) {
        return `${substateId.NonFungibleIndex.resource_address}:${substateId.NonFungibleIndex.index}`;
    }
    if ("TransactionReceipt" in substateId) {
        return substateId.TransactionReceipt;
    }
    if ("ValidatorFeePool" in substateId) {
        return substateId.ValidatorFeePool;
    }
    console.error("Unknown substate id", substateId);
    return "Unknown";
}
export function stringToSubstateId(substateId) {
    const parts = splitOnce(substateId, "_");
    if (!parts) {
        throw new Error(`Invalid substate id: ${substateId}`);
    }
    switch (parts[0]) {
        case "component":
            return { Component: parts[1] };
        case "resource":
            if (parts[1].includes(" nft_")) {
                return { NonFungible: parts[1] };
            }
            return { Resource: parts[1] };
        case "vault":
            return { Vault: parts[1] };
        case "commitment":
            return { UnclaimedConfidentialOutput: parts[1] };
        case "txreceipt":
            return { TransactionReceipt: parts[1] };
        case "vnfp":
            return { ValidatorFeePool: parts[1] };
        default:
            throw new Error(`Unknown substate id: ${substateId}`);
    }
}
export function shortenSubstateId(substateId, start = 4, end = 4) {
    if (substateId === null || substateId === undefined) {
        return "";
    }
    const string = substateIdToString(substateId);
    const parts = string.split("_", 2);
    if (parts.length < 2) {
        return string;
    }
    return parts[0] + "_" + shortenString(parts[1], start, end);
}
export function shortenString(string, start = 8, end = 8) {
    return string.substring(0, start) + "..." + string.slice(-end);
}
export function rejectReasonToString(reason) {
    if (reason === null) {
        return "";
    }
    if (typeof reason === "string") {
        return reason;
    }
    if ("ShardsNotPledged" in reason) {
        return `ShardsNotPledged(${reason.ShardsNotPledged})`;
    }
    if ("ExecutionFailure" in reason) {
        return `ExecutionFailure(${reason.ExecutionFailure})`;
    }
    if ("ShardPledgedToAnotherPayload" in reason) {
        return `ShardPledgedToAnotherPayload(${reason.ShardPledgedToAnotherPayload})`;
    }
    if ("ShardRejected" in reason) {
        return `ShardRejected(${reason.ShardRejected})`;
    }
    if ("FeesNotPaid" in reason) {
        return `FeesNotPaid(${reason.FeesNotPaid})`;
    }
    if ("ForeignShardGroupDecidedToAbort" in reason) {
        const r = reason.ForeignShardGroupDecidedToAbort;
        return `ForeignShardGroupDecidedToAbort(${r.start_shard}-${r.end_shard}, ${r.abort_reason})`;
    }
    if ("InvalidTransaction" in reason) {
        return `InvalidTransaction(${reason.InvalidTransaction})`;
    }
    if ("ExecutionFailure" in reason) {
        return `ExecutionFailure(${reason.ExecutionFailure})`;
    }
    if ("OneOrMoreInputsNotFound" in reason) {
        return `OneOrMoreInputsNotFound(${reason.OneOrMoreInputsNotFound})`;
    }
    if ("FailedToLockInputs" in reason) {
        return `FailedToLockInputs(${reason.FailedToLockInputs})`;
    }
    if ("FailedToLockOutputs" in reason) {
        return `FailedToLockOutputs(${reason.FailedToLockOutputs})`;
    }
    console.error("Unknown reason", reason);
    return JSON.stringify(reason);
}
export function getSubstateDiffFromTransactionResult(result) {
    if ("Accept" in result) {
        return result.Accept;
    }
    if ("AcceptFeeRejectRest" in result) {
        return result.AcceptFeeRejectRest[0];
    }
    return null;
}
export function getRejectReasonFromTransactionResult(result) {
    if ("Reject" in result) {
        return result.Reject;
    }
    if ("AcceptFeeRejectRest" in result) {
        return result.AcceptFeeRejectRest[1];
    }
    return null;
}
export function jrpcPermissionToString(jrpcPermission) {
    if (typeof jrpcPermission === "string") {
        return jrpcPermission;
    }
    if ("NftGetOwnershipProof" in jrpcPermission) {
        return `NftGetOwnershipProof(${jrpcPermission.NftGetOwnershipProof})`;
    }
    if ("AccountBalance" in jrpcPermission) {
        return `AccountBalance(${substateIdToString(jrpcPermission.AccountBalance)})`;
    }
    if ("AccountList" in jrpcPermission) {
        return `AccountList(${jrpcPermission.AccountList})`;
    }
    if ("TransactionSend" in jrpcPermission) {
        return `TransactionSend(${jrpcPermission.TransactionSend})`;
    }
    if ("GetNft" in jrpcPermission) {
        return `GetNft(${substateIdToString(jrpcPermission.GetNft[0])}, ${jrpcPermission.GetNft[1]})`;
    }
    return "Unknown";
}
function splitOnce(str, separator) {
    const index = str.indexOf(separator);
    if (index === -1) {
        return null;
    }
    return [str.slice(0, index), str.slice(index + 1)];
}
