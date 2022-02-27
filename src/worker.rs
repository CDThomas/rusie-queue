use crate::postgres::{PostgresJob, PostgresQueue};
use futures::{stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

const CONCURRENCY: usize = 50;

pub async fn run<M, E, F, Fut>(queue: Arc<PostgresQueue>, handler: F)
where
    M: Serialize + DeserializeOwned + Unpin + Send + 'static,
    E: Display,
    F: Fn(PostgresJob<M>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>>,
{
    loop {
        let jobs = match queue.pull::<M>(CONCURRENCY as u32).await {
            Ok(jobs) => jobs,
            Err(err) => {
                println!("run: pulling jobs: {}", err);
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

                let res = match handler(job).await {
                    Ok(_) => queue.delete_job(job_id).await,
                    Err(err) => {
                        println!("run: handling job({}): {}", job_id, &err);
                        queue.fail_job(job_id).await
                    }
                };

                match res {
                    Ok(_) => {}
                    Err(err) => {
                        println!("run: deleting / failing job: {}", &err);
                    }
                }
            })
            .await;

        // sleep not to overload the database
        tokio::time::sleep(Duration::from_millis(125)).await;
    }
}
