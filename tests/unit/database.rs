#![cfg(feature = "sqlite")]

use crate::{
    backend::sqlite::SqliteTaskRepo,
    database::Database,
    error::StorageError,
    task::{Task, TaskStatus},
};
use serde_json::json;
#[path = "graph_fixture.rs"]
mod graph_fixture;

fn repository(database: &Database) -> SqliteTaskRepo {
    match database {
        Database::Sqlite(pool) => SqliteTaskRepo::new(
            pool.clone(),
            graph_fixture::graph(
                "connection-check",
                &[("start", crate::graph::TaskType::Normal, None)],
                &[],
            ),
        ),
        #[cfg(feature = "pg")]
        Database::Postgres(_) => panic!("SQLite URL must select SQLite"),
    }
}

fn task() -> Task {
    Task::new("connection-check", "start", json!({"persisted": true}))
}

#[tokio::test]
async fn memory_and_temporary_urls_keep_tasks_until_shared_close() {
    for url in [
        "sqlite::memory:",
        "sqlite://:memory:",
        "sqlite://?mode=memory",
        "sqlite://?%6dode=%6demory",
        "sqlite:",
        "sqlite://",
    ] {
        let database = Database::connect(url).await.unwrap();
        database.initialize().await.unwrap();
        let shared = database.clone();
        let inserted = database.submit(task()).await.unwrap();
        shared.check().await.unwrap();
        let loaded = repository(&shared)
            .get(&inserted.workflow_id, inserted.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.input, task().input, "{url}");
        assert_eq!(loaded.status, TaskStatus::Pending);
        database.close().await;
        assert!(shared.check().await.is_err(), "{url}");
    }
}

#[tokio::test]
async fn file_urls_persist_tasks_and_respect_explicit_access_modes() {
    let path =
        std::env::temp_dir().join(format!("workflow-connection-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());

    assert!(Database::connect(&format!("{url}?mode=rw")).await.is_err());
    assert!(Database::connect(&format!("{url}?mode=ro")).await.is_err());
    assert!(
        !path.exists(),
        "explicit modes must not create a missing file"
    );

    let database = Database::connect(&url).await.unwrap();
    database.initialize().await.unwrap();
    let inserted = database.submit(task()).await.unwrap();
    database.close().await;

    let database = Database::connect(&format!("{url}?mode=ro")).await.unwrap();
    database.check().await.unwrap();
    let loaded = repository(&database)
        .get(&inserted.workflow_id, inserted.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.input, task().input);
    assert!(database.submit(task()).await.is_err());
    database.close().await;
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn unsupported_and_disabled_backends_return_explicit_errors() {
    assert!(matches!(
        Database::connect("unknown://example").await,
        Err(StorageError::UnsupportedScheme)
    ));
    #[cfg(not(feature = "pg"))]
    assert!(matches!(
        Database::connect("postgres://localhost/unused").await,
        Err(StorageError::BackendDisabled {
            backend: "postgresql"
        })
    ));
}
