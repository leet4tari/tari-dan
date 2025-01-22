export type AbortReason = "None" | "TransactionAtomMustBeAbort" | "TransactionAtomMustBeCommit" | "InputLockConflict" | "LockInputsFailed" | "LockOutputsFailed" | "LockInputsOutputsFailed" | "InvalidTransaction" | "ExecutionFailure" | "OneOrMoreInputsNotFound" | "ForeignShardGroupDecidedToAbort" | "FeesNotPaid" | "EarlyAbort";
