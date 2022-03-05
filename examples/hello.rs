use clap::{Parser, Subcommand};
use rusie_queue::error::Error;
use rusie_queue::postgres::{PostgresJob, PostgresQueue};
use rusie_queue::worker;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::time::Duration;

#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Push { names: Vec<String> },
    Worker,
}

#[derive(Serialize, Deserialize)]
pub enum Message {
    SayHello { name: String },
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();

    let database_url = std::env::var("DATABASE_URL")
        .map_err(|_| Error::BadConfig("DATABASE_URL env var is missing".to_string()))?;

    let db = connect(&database_url).await?;

    migrate(&db).await?;

    let queue = PostgresQueue::new(db.clone()).max_attempts(3);

    match cli.command {
        Commands::Push { names } => {
            if names.is_empty() {
                let message = Message::SayHello {
                    name: "Rusie Q".to_string(),
                };

                queue.push(message, None).await?;
            } else {
                for name in names {
                    queue.push(Message::SayHello { name }, None).await?
                }
            }
        }
        Commands::Worker => {
            println!("Starting worker and waiting for messages...");
            worker::run(queue, handle_job).await;
        }
    }

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
