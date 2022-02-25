use futures::{stream, StreamExt};
use rusie_queue::error::Error;
use rusie_queue::postgres::*;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

const CONCURRENCY: usize = 50;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Message {
    SendSignInEmail {
        email: String,
        name: String,
        code: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let database_url = std::env::var("DATABASE_URL")
        .map_err(|_| Error::BadConfig("DATABASE_URL env var is missing".to_string()))?;

    let db = connect(&database_url).await?;
    migrate(&db).await?;

    let queue = Arc::new(PostgresQueue::new(db.clone()).max_attempts(3));

    // run worker
    let worker_queue = queue.clone(); // queue is an Arc pointer, so we only copy the reference
    tokio::spawn(async move { run_worker(worker_queue).await });

    // queue job
    let job = Message::SendSignInEmail {
        email: "your@email.com".to_string(),
        name: "Rusie Q".to_string(),
        code: "000-000".to_string(),
    };
    let _ = queue.push(job, None).await; // TODO: handle error

    tokio::time::sleep(Duration::from_secs(2)).await;

    Ok(())
}

// TODO: move into rusie_queue
async fn run_worker(queue: Arc<PostgresQueue>) {
    loop {
        let jobs = match queue.pull::<Message>(CONCURRENCY as u32).await {
            Ok(jobs) => jobs,
            Err(err) => {
                println!("run_worker: pulling jobs: {}", err);
                tokio::time::sleep(Duration::from_millis(500)).await;
                Vec::new()
            }
        };

        let number_of_jobs = jobs.len();
        if number_of_jobs > 0 {
            println!("Fetched {} jobs", number_of_jobs);
        }

        stream::iter(jobs)
            .for_each_concurrent(CONCURRENCY, |job| async {
                let job_id = job.id;

                let res = match handle_job(job).await {
                    Ok(_) => queue.delete_job(job_id).await,
                    Err(err) => {
                        println!("run_worker: handling job({}): {}", job_id, &err);
                        queue.fail_job(job_id).await
                    }
                };

                match res {
                    Ok(_) => {}
                    Err(err) => {
                        println!("run_worker: deleting / failing job: {}", &err);
                    }
                }
            })
            .await;

        // sleep not to overload our database
        tokio::time::sleep(Duration::from_millis(125)).await;
    }
}

async fn handle_job(job: PostgresJob<Message>) -> Result<(), crate::Error> {
    match job.message.0 {
        message @ Message::SendSignInEmail { .. } => {
            println!("Sending sign in email: {:?}", &message);
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
