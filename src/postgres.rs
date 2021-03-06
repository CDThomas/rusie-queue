use crate::error::Error;
use chrono;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::types::Json;
use sqlx::{Pool, Postgres};
use ulid::Ulid;
use uuid::Uuid;

type DB = Pool<Postgres>;

const DEFAULT_MAX_ATTEMPTS: u32 = 5;

#[derive(Debug, Clone)]
pub struct PostgresQueue {
    pub(crate) db: DB,
    pub(crate) max_attempts: u32,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct PostgresJob<A: Serialize + DeserializeOwned> {
    pub id: Uuid,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,

    pub scheduled_for: chrono::DateTime<chrono::Utc>,
    pub failed_attempts: i32,
    pub status: PostgresJobStatus,
    pub message: Json<A>,
}

#[derive(Debug, Clone, sqlx::Type, PartialEq)]
#[repr(i32)]
pub enum PostgresJobStatus {
    Queued,
    Running,
}

impl PostgresQueue {
    pub fn new(db: DB) -> PostgresQueue {
        PostgresQueue {
            db,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        }
    }

    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }
}

impl PostgresQueue {
    pub async fn push<A: Serialize + Send>(
        &self,
        job: A,
        date: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), Error> {
        let scheduled_for = date.unwrap_or_else(chrono::Utc::now);
        let failed_attempts: i32 = 0;
        let message = Json(job);
        let status = PostgresJobStatus::Queued;
        let now = chrono::Utc::now();
        let job_id: Uuid = Ulid::new().into();
        let query = "INSERT INTO queue
            (id, created_at, updated_at, scheduled_for, failed_attempts, status, message)
            VALUES ($1, $2, $3, $4, $5, $6, $7)";

        sqlx::query(query)
            .bind(job_id)
            .bind(now)
            .bind(now)
            .bind(scheduled_for)
            .bind(failed_attempts)
            .bind(status)
            .bind(message)
            .execute(&self.db)
            .await?;

        Ok(())
    }

    pub async fn delete_job(&self, job_id: Uuid) -> Result<(), Error> {
        let query = "DELETE FROM queue WHERE id = $1";
        sqlx::query(query).bind(job_id).execute(&self.db).await?;

        Ok(())
    }

    pub async fn fail_job(&self, job_id: Uuid) -> Result<(), Error> {
        let now = chrono::Utc::now();
        let query = "UPDATE queue
            SET status = $1, updated_at = $2, failed_attempts = failed_attempts + 1
            WHERE id = $3";

        sqlx::query(query)
            .bind(PostgresJobStatus::Queued)
            .bind(now)
            .bind(job_id)
            .execute(&self.db)
            .await?;

        Ok(())
    }

    pub async fn pull<A: Serialize + DeserializeOwned + Unpin + Send + 'static>(
        &self,
        number_of_jobs: u32,
    ) -> Result<Vec<PostgresJob<A>>, Error> {
        let number_of_jobs = if number_of_jobs > 100 {
            100
        } else {
            number_of_jobs
        };
        let now = chrono::Utc::now();
        let query = "UPDATE queue
            SET status = $1, updated_at = $2
            WHERE id IN (
                SELECT id
                FROM queue
                WHERE status = $3 AND scheduled_for <= $4 AND failed_attempts < $5
                ORDER BY scheduled_for
                FOR UPDATE SKIP LOCKED
                LIMIT $6
            )
            RETURNING *";

        let jobs: Vec<PostgresJob<A>> = sqlx::query_as(query)
            .bind(PostgresJobStatus::Running)
            .bind(now)
            .bind(PostgresJobStatus::Queued)
            .bind(now)
            .bind(self.max_attempts)
            .bind(number_of_jobs)
            .fetch_all(&self.db)
            .await?;

        Ok(jobs.into_iter().map(Into::into).collect())
    }

    pub async fn clear(&self) -> Result<(), Error> {
        let query = "DELETE FROM queue";

        sqlx::query(query).execute(&self.db).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::SubsecRound;
    use serde::Deserialize;
    use sqlx::postgres::PgPoolOptions;
    use std::time::Duration;

    #[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
    pub enum Message {
        SendSignInEmail {
            email: String,
            name: String,
            code: String,
        },
    }

    struct Context {
        db: DB,
        queue: PostgresQueue,
        message: Message,
    }

    pub async fn connect(database_url: &str) -> Result<DB, sqlx::Error> {
        PgPoolOptions::new()
            .max_connections(100)
            .max_lifetime(Duration::from_secs(30 * 60)) // 30 mins
            .connect(database_url)
            .await
    }

    async fn setup_db() -> DB {
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL not set");

        let db = connect(&database_url)
            .await
            .expect("failed to connect to DB");

        // TODO: use a transaction rather than truncating so that tests can run in parallel.
        // Have to run tests with `cargo test -- --test-threads=1` if using `TRUNCATE` before
        // each test.
        sqlx::query("TRUNCATE TABLE queue")
            .execute(&db)
            .await
            .expect("failed to truncate queue table");

        db
    }

    async fn setup() -> Context {
        let db = setup_db().await;
        let queue = PostgresQueue::new(db.clone());
        let message = Message::SendSignInEmail {
            email: String::from("test@test.com"),
            name: String::from("Drew"),
            code: String::from("abc"),
        };

        Context { queue, db, message }
    }

    async fn all_jobs<A: Serialize + DeserializeOwned + Send + Unpin + 'static>(
        db: &DB,
    ) -> Result<Vec<PostgresJob<A>>, Error> {
        let jobs = sqlx::query_as::<_, PostgresJob<A>>("SELECT * FROM queue")
            .fetch_all(db)
            .await?;

        Ok(jobs)
    }

    #[tokio::test]
    async fn test_new_sets_default_max_attempts() {
        let db = setup_db().await;
        let queue = PostgresQueue::new(db);

        assert_eq!(queue.max_attempts, DEFAULT_MAX_ATTEMPTS);
    }

    #[tokio::test]
    async fn test_max_attempts_updates_field() {
        let db = setup_db().await;
        let max_attempts = DEFAULT_MAX_ATTEMPTS + 1;

        let queue = PostgresQueue::new(db).max_attempts(max_attempts);

        assert_eq!(queue.max_attempts, max_attempts);
    }

    #[tokio::test]
    async fn test_push_succeeds() {
        let Context { queue, message, .. } = setup().await;

        let result = queue.push(message, None).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_push_pushes_job_to_queue() {
        let Context { queue, db, message } = setup().await;

        let scheduled_for = chrono::Utc::now() + chrono::Duration::seconds(10);

        queue
            .push(message.clone(), Some(scheduled_for))
            .await
            .expect("push failed");

        let jobs = all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch all jobs");

        assert_eq!(jobs.len(), 1);

        // Timestamps in PG have usec precision. Truncating the timestamps from
        // chrono ensures that tests won't fail because of precision mismatch on
        // platforms that return a higher precision than usec for now().
        assert_eq!(jobs[0].scheduled_for, scheduled_for.trunc_subsecs(6));
        assert_eq!(jobs[0].failed_attempts, 0);
        assert_eq!(jobs[0].status, PostgresJobStatus::Queued);
        assert_eq!(jobs[0].message.0, message);
    }

    #[tokio::test]
    async fn test_delete_deletes_job() {
        let Context { queue, db, message } = setup().await;

        queue.push(message, None).await.expect("push failed");

        let jobs = all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch all jobs");

        queue.delete_job(jobs[0].id).await.expect("delete failed");

        let result = sqlx::query!("SELECT COUNT(*) as job_count FROM queue")
            .fetch_one(&db)
            .await
            .expect("count failed");

        assert_eq!(result.job_count.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_delete_deletes_only_given_job() {
        let Context { queue, db, message } = setup().await;

        queue
            .push(message.clone(), None)
            .await
            .expect("push failed");

        queue.push(message, None).await.expect("push failed");

        let jobs_before_delete = all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch all jobs");

        assert_eq!(jobs_before_delete.len(), 2);

        queue
            .delete_job(jobs_before_delete[0].id)
            .await
            .expect("delete failed");

        let jobs_after_delete = all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch all jobs");

        assert_eq!(jobs_after_delete.len(), 1);
        assert_eq!(jobs_after_delete[0].id, jobs_before_delete[1].id);
    }

    #[tokio::test]
    async fn test_clear_clears_all_jobs() {
        let Context { queue, db, message } = setup().await;

        queue
            .push(message.clone(), None)
            .await
            .expect("push failed");
        queue.push(message, None).await.expect("push failed");

        queue.clear().await.expect("clear failed");

        let result = sqlx::query!("SELECT COUNT(*) as job_count FROM queue")
            .fetch_one(&db)
            .await
            .expect("count failed");

        assert_eq!(result.job_count.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_fail_fails_given_job() {
        let Context { queue, db, message } = setup().await;

        queue.push(message, None).await.expect("push failed");

        let job = &all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch all jobs")[0];

        assert_eq!(job.failed_attempts, 0);
        assert_eq!(job.status, PostgresJobStatus::Queued);

        queue.fail_job(job.id).await.expect("failed to fail job");

        let failed_job = &all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch all jobs")[0];

        assert_eq!(failed_job.failed_attempts, 1);
        assert_eq!(failed_job.status, PostgresJobStatus::Queued);
        assert!(failed_job.updated_at > job.updated_at);
    }

    #[tokio::test]
    async fn test_pull_pulls_correct_number_of_jobs() {
        let Context { queue, message, .. } = setup().await;

        queue
            .push(message.clone(), None)
            .await
            .expect("push failed");
        queue.push(message, None).await.expect("push failed");

        let jobs = queue.pull::<Message>(1).await.expect("failed to pull jobs");

        assert_eq!(jobs.len(), 1);
    }

    #[tokio::test]
    async fn test_pull_updates_status_to_running() {
        let Context { queue, db, message } = setup().await;

        queue.push(message, None).await.expect("push failed");

        let job = &queue.pull::<Message>(1).await.expect("failed to pull jobs")[0];

        let db_job = &all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch jobs")[0];

        assert_eq!(job.id, db_job.id);
        assert_eq!(db_job.status, PostgresJobStatus::Running);
    }

    #[tokio::test]
    async fn test_pull_updates_updated_at_timestamp() {
        let Context { queue, db, message } = setup().await;

        queue.push(message, None).await.expect("push failed");

        let db_job_before_pull = &all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch jobs")[0];

        let job = &queue.pull::<Message>(1).await.expect("failed to pull jobs")[0];

        let db_job_after_pull = &all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch jobs")[0];

        assert_eq!(job.id, db_job_before_pull.id);
        assert_eq!(db_job_after_pull.id, db_job_before_pull.id);
        assert!(db_job_before_pull.updated_at < db_job_after_pull.updated_at);
    }

    #[tokio::test]
    async fn test_pull_only_pulls_queued_jobs() {
        let Context { queue, db, message } = setup().await;

        queue
            .push(message.clone(), None)
            .await
            .expect("push failed");
        queue.push(message, None).await.expect("push failed");

        let db_jobs = all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch jobs");

        let job_1 = &queue.pull::<Message>(1).await.expect("failed to pull jobs")[0];
        let job_2 = &queue.pull::<Message>(1).await.expect("failed to pull jobs")[0];

        assert_eq!(db_jobs[0].id, job_1.id);
        assert_eq!(db_jobs[1].id, job_2.id);
    }

    #[tokio::test]
    async fn test_pull_pulls_queued_jobs_in_scheduled_order() {
        let Context { queue, db, message } = setup().await;
        let ten_seconds_ago = chrono::Utc::now() - chrono::Duration::seconds(10);
        let five_seconds_ago = chrono::Utc::now() - chrono::Duration::seconds(5);

        queue
            .push(message.clone(), Some(five_seconds_ago))
            .await
            .expect("push failed");
        queue
            .push(message, Some(ten_seconds_ago))
            .await
            .expect("push failed");

        let db_jobs = all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch jobs");

        let job_1 = &queue.pull::<Message>(1).await.expect("failed to pull jobs")[0];
        let job_2 = &queue.pull::<Message>(1).await.expect("failed to pull jobs")[0];

        // Timestamps in PG have usec precision. Truncating the timestamps from
        // chrono ensures that tests won't fail because of precision mismatch on
        // platforms that return a higher precision than usec for now().
        assert_eq!(db_jobs[1].id, job_1.id);
        assert_eq!(db_jobs[1].scheduled_for, ten_seconds_ago.trunc_subsecs(6));
        assert_eq!(db_jobs[0].id, job_2.id);
        assert_eq!(db_jobs[0].scheduled_for, five_seconds_ago.trunc_subsecs(6));
    }

    #[tokio::test]
    async fn test_pull_only_pulls_scheduled_for_less_than_now() {
        let Context { queue, message, .. } = setup().await;
        let one_min_from_now = chrono::Utc::now() + chrono::Duration::minutes(1);

        queue
            .push(message.clone(), Some(one_min_from_now))
            .await
            .expect("push failed");

        let jobs = queue.pull::<Message>(1).await.expect("failed to pull jobs");

        assert_eq!(jobs.len(), 0);
    }

    #[tokio::test]
    async fn test_pull_pulls_jobs_that_have_failed_less_than_max_attempts() {
        let Context { queue, db, message } = setup().await;

        queue.push(message, None).await.expect("push failed");

        let db_job = &all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch all jobs")[0];

        queue.fail_job(db_job.id).await.expect("failed to fail job");

        let job = &queue.pull::<Message>(1).await.expect("failed to pull jobs")[0];

        assert_eq!(job.id, db_job.id);
    }

    #[tokio::test]
    async fn test_pull_does_not_pull_jobs_that_have_failed_equal_to_max_attempts() {
        let Context { queue, db, message } = setup().await;

        queue.push(message, None).await.expect("push failed");

        let db_job = &all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch all jobs")[0];

        for _ in 0..queue.max_attempts {
            queue.fail_job(db_job.id).await.expect("failed to fail job");
        }

        let db_job_after_fail = &all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch all jobs")[0];

        let jobs = &queue.pull::<Message>(1).await.expect("failed to pull jobs");

        assert_eq!(db_job_after_fail.failed_attempts, queue.max_attempts as i32);
        assert_eq!(jobs.len(), 0);
    }

    #[tokio::test]
    async fn test_pull_does_not_pull_jobs_that_have_failed_more_than_max_attempts() {
        let Context { queue, db, message } = setup().await;

        queue.push(message, None).await.expect("push failed");

        let db_job = &all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch all jobs")[0];

        for _ in 0..queue.max_attempts + 1 {
            queue.fail_job(db_job.id).await.expect("failed to fail job");
        }

        let db_job_after_fail = &all_jobs::<Message>(&db)
            .await
            .expect("failed to fetch all jobs")[0];

        let jobs = &queue.pull::<Message>(1).await.expect("failed to pull jobs");

        assert!(db_job_after_fail.failed_attempts > queue.max_attempts as i32);
        assert_eq!(jobs.len(), 0);
    }
}
