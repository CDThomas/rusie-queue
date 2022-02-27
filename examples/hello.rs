use rusie_queue::error::Error;
use rusie_queue::postgres::{PostgresJob, PostgresQueue};
use rusie_queue::worker;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
pub enum Message {
    SayHello { name: String },
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let database_url = std::env::var("DATABASE_URL")
        .map_err(|_| Error::BadConfig("DATABASE_URL env var is missing".to_string()))?;

    let db = connect(&database_url).await?;
    migrate(&db).await?;

    let queue = Arc::new(PostgresQueue::new(db.clone()).max_attempts(3));

    let worker_queue = queue.clone();
    tokio::spawn(async move { worker::run(worker_queue, handle_job).await });

    let job = Message::SayHello {
        name: "Rusie Q".to_string(),
    };

    queue.push(job, None).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    Ok(())
}

async fn handle_job(job: PostgresJob<Message>) -> Result<(), anyhow::Error> {
    match job.message.0 {
        Message::SayHello { name } => {
            println!("Hello, {}", name);
        }
    };

    Ok(())
}

async fn connect(database_url: &str) -> Result<Pool<Postgres>, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(100)
        .max_lifetime(Duration::from_secs(30 * 60)) // 30 mins
        .connect(database_url)
        .await
}

pub async fn migrate(db: &Pool<Postgres>) -> Result<(), sqlx::Error> {
    sqlx::migrate!("./migrations").run(db).await?;
    Ok(())
}
