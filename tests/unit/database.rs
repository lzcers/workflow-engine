#![cfg(feature = "sqlite")]

use crate::{
    backend::sqlite::SqliteTaskRepo,
    database::Database,
    error::StorageError,
    task::{Task, TaskStatus},
};
use serde_json::json;

fn repository(database: &Database) -> SqliteTaskRepo {
    match database {
        Database::Sqlite(pool) => SqliteTaskRepo::new(pool.clone()),
        #[cfg(feature = "pg")]
        Database::Postgres(_) => panic!("SQLite URL must select SQLite"),
    }
}

fn task() -> Task {
    Task {
        task_type: "normal".into(),
        workflow_id: "connection-check".into(),
        run_id: Some(1),
        parent_task_id: None,
        name: "start".into(),
        max_attempts: 3,
        depends_on_ids: vec![],
        depends_on_names: vec![],
        input: json!({"persisted": true}),
    }
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
        let inserted = repository(&database).enqueue(&task()).await.unwrap();
        shared.check().await.unwrap();
        let loaded = repository(&shared)
            .get(&inserted.workflow_id, inserted.task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.input, inserted.input, "{url}");
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
    let inserted = repository(&database).enqueue(&task()).await.unwrap();
    database.close().await;

    let database = Database::connect(&format!("{url}?mode=ro")).await.unwrap();
    database.check().await.unwrap();
    let loaded = repository(&database)
        .get(&inserted.workflow_id, inserted.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.input, inserted.input);
    assert!(repository(&database).enqueue(&task()).await.is_err());
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
