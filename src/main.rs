mod db;
mod error;
mod postgres;
mod queue;
use std::{time::Duration};
use serde::Serialize;


pub use error::Error;
use postgres::{PostgresQueue};
use queue::{Queue, Task};

#[derive(Debug, Serialize)]
struct SomeTask {
    a: u32,
    b: u32,
}

// Better names? Message and message.handle?
#[async_trait::async_trait]
impl Task for SomeTask {
    async fn run(&self) -> Result<(), crate::Error> {
        println!("Running with args: {:?}", self);
        return ();
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let database_url = std::env::var("DATABASE_URL")
        .map_err(|_| Error::BadConfig("DATABASE_URL env var is missing".to_string()))?;

    let db = db::connect(&database_url).await?;

    db::migrate(&db).await?;

    let queue = PostgresQueue::new(db.clone()).max_attempts(3);

    queue.push(SomeTask::new(1, 2)).await?;

    tokio::spawn(async move { queue.run_worker().await });

    tokio::time::sleep(Duration::from_secs(5)).await;

    Ok(())
}
