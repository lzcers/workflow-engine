//! 引擎统一的存储与构图错误类型。

/// 所有持久化后端共同的错误类型。
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("claim for task {workflow_id}:{task_id} is no longer valid")]
    ClaimLost { workflow_id: String, task_id: i64 },
    #[error("invalid fanout plan: {detail}")]
    InvalidFanout { detail: String },
    #[error("run {workflow_id}:{run_id} exceeded max tasks limit ({limit})")]
    RunTaskLimitExceeded {
        workflow_id: String,
        run_id: i64,
        limit: usize,
    },
    #[error("unsupported database URL scheme; expected postgres, postgresql, or sqlite")]
    UnsupportedScheme,
    #[error("database backend {backend} is not enabled")]
    BackendDisabled { backend: &'static str },
    #[error("{0}")]
    Other(String),
}

/// 静态构图与拓扑校验错误。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GraphError {
    #[error("workflow_id must not be empty")]
    EmptyWorkflowId,
    #[error("workflow title must not be empty")]
    EmptyTitle,
    #[error("duplicate node id: {0}")]
    DuplicateNode(String),
    #[error("duplicate subgraph id: {0}")]
    DuplicateSubgraph(String),
    #[error("unknown node: {0}")]
    UnknownNode(String),
    #[error("unknown subgraph: {0}")]
    UnknownSubgraph(String),
    #[error("subgraph id and title must not be empty")]
    EmptySubgraphMetadata,
    #[error("{kind} {id} must use its full parent-qualified name")]
    InvalidQualification { kind: &'static str, id: String },
    #[error("edge {src} -> {dst}: {detail}")]
    InvalidEdge {
        src: String,
        dst: String,
        detail: String,
    },
    #[error("join node {node}: {detail}")]
    InvalidJoin { node: String, detail: String },
    #[error("success subgraph contains a cycle")]
    SuccessCycle,
    #[error("failure edge {src} -> {dst} must not loop back to itself")]
    FailureSelfLoop { src: String, dst: String },
}
