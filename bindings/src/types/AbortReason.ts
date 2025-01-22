// This file was generated by [ts-rs](https://github.com/Aleph-Alpha/ts-rs). Do not edit this file manually.

export type AbortReason =
  | "None"
  | "TransactionAtomMustBeAbort"
  | "TransactionAtomMustBeCommit"
  | "InputLockConflict"
  | "LockInputsFailed"
  | "LockOutputsFailed"
  | "LockInputsOutputsFailed"
  | "InvalidTransaction"
  | "ExecutionFailure"
  | "OneOrMoreInputsNotFound"
  | "ForeignShardGroupDecidedToAbort"
  | "FeesNotPaid"
  | "EarlyAbort";
