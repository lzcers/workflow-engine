//! 所有持久化后端共同的错误类型。

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("task {workflow_id}:{task_id} missing after settle")]
    MissingTask { workflow_id: String, task_id: i64 },
    #[error("unsupported database URL scheme; expected postgres, postgresql, or sqlite")]
    UnsupportedScheme,
    #[error("database backend {backend} is not enabled")]
    BackendDisabled { backend: &'static str },
}
