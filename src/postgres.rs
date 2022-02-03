use crate::{
    db::DB,
    queue::{Job, Message, Queue},
};
use chrono;
use sqlx::{self, types::Json};
use ulid::Ulid;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PostgresQueue {
    db: DB,
    max_attempts: u32,
}

const MAX_FAILED_ATTEMPTS: i32 = 3; // low, as most jobs also use retries internally

#[derive(sqlx::FromRow, Debug, Clone)]
struct PostgresJob {
    id: Uuid,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,

    scheduled_for: chrono::DateTime<chrono::Utc>,
    failed_attempts: i32,
    status: PostgresJobStatus,
    message: Json<Message>,
}

// We use a INT postgres representation for performance reasons
#[derive(Debug, Clone, sqlx::Type, PartialEq)]
#[repr(i32)]
enum PostgresJobStatus {
    Queued,
    Running,
}

impl From<PostgresJob> for Job {
    fn from(item: PostgresJob) -> Self {
        Job {
            id: item.id,
            message: item.message.0,
        }
    }
}

impl PostgresQueue {
    pub fn new(db: DB) -> PostgresQueue {
        let queue = PostgresQueue {
            db,
            max_attempts: 5,
        };

        queue
    }
}

#[async_trait::async_trait]
impl Queue for PostgresQueue {
    async fn push(
        &self,
        job: Message,
        date: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), crate::Error> {
        let scheduled_for = date.unwrap_or(chrono::Utc::now());
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

    async fn delete_job(&self, job_id: Uuid) -> Result<(), crate::Error> {
        let query = "DELETE FROM queue WHERE id = $1";
        sqlx::query(query).bind(job_id).execute(&self.db).await?;

        Ok(())
    }

    async fn fail_job(&self, job_id: Uuid) -> Result<(), crate::Error> {
        let now = chrono::Utc::now();
        let query = "UPDATE queue
            SET status = $1, updated_at = $2, failed_attempts = failed_attempts + 1
            WHERE id = $3";

        sqlx::query(query)
            // TODO: does status really need to be set at all here?
            .bind(PostgresJobStatus::Queued)
            .bind(now)
            .bind(job_id)
            .execute(&self.db)
            .await?;

        Ok(())
    }

    // TODO: test
    //  * pulls correct number of jobs
    //  * only pulls jobs with correct status, scheduled for, and failed attempts
    //  * returns jobs to run
    //  * Status of jobs is updated
    //  * Updated at is updated
    //  * Subsequent pulls will not pull the same job
    //  * Concurrent pulls can't pull the same job
    //    * Might test this at the integration level and start a few workers and make
    //      sure that all of the runs are unique. Retries could be on different workers
    //      though.
    //  *
    async fn pull(&self, number_of_jobs: u32) -> Result<Vec<Job>, crate::Error> {
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

        // Why query_as into a PG job and then map into Job?
        let jobs: Vec<PostgresJob> = sqlx::query_as::<_, PostgresJob>(query)
            .bind(PostgresJobStatus::Running)
            .bind(now)
            .bind(PostgresJobStatus::Queued)
            .bind(now)
            // TODO: use config val rather than const
            .bind(MAX_FAILED_ATTEMPTS)
            .bind(number_of_jobs)
            .fetch_all(&self.db)
            .await?;

        Ok(jobs.into_iter().map(Into::into).collect())
    }

    async fn clear(&self) -> Result<(), crate::Error> {
        let query = "DELETE FROM queue";

        sqlx::query(query).execute(&self.db).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    struct Context {
        db: DB,
        queue: PostgresQueue,
        message: Message,
    }

    async fn setup_db() -> DB {
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL not set");

        let db = db::connect(&database_url)
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

    async fn all_jobs(db: &DB) -> Result<Vec<PostgresJob>, crate::Error> {
        let jobs = sqlx::query_as::<_, PostgresJob>("SELECT * FROM queue")
            .fetch_all(db)
            .await?;

        Ok(jobs)
    }

    #[tokio::test]
    async fn test_push_succeeds() {
        let Context { queue, message, .. } = setup().await;

        let result = queue.push(message, None).await.expect("push failed");

        assert_eq!(result, ());
    }

    #[tokio::test]
    async fn test_push_pushes_job_to_queue() {
        let Context { queue, db, message } = setup().await;

        let scheduled_for = chrono::Utc::now() + chrono::Duration::seconds(10);

        queue
            .push(message.clone(), Some(scheduled_for))
            .await
            .expect("push failed");

        let jobs = all_jobs(&db).await.expect("failed to fetch all jobs");

        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].scheduled_for, scheduled_for);
        assert_eq!(jobs[0].failed_attempts, 0);
        assert_eq!(jobs[0].status, PostgresJobStatus::Queued);
        assert_eq!(jobs[0].message.0, message);
    }

    #[tokio::test]
    async fn test_delete_deletes_job() {
        let Context { queue, db, message } = setup().await;

        queue.push(message, None).await.expect("push failed");

        let jobs = all_jobs(&db).await.expect("failed to fetch all jobs");

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

        let jobs_before_delete = all_jobs(&db).await.expect("failed to fetch all jobs");

        assert_eq!(jobs_before_delete.len(), 2);

        queue
            .delete_job(jobs_before_delete[0].id)
            .await
            .expect("delete failed");

        let jobs_after_delete = all_jobs(&db).await.expect("failed to fetch all jobs");

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

        let job = &all_jobs(&db).await.expect("failed to fetch all jobs")[0];

        assert_eq!(job.failed_attempts, 0);
        assert_eq!(job.status, PostgresJobStatus::Queued);

        queue.fail_job(job.id).await.expect("failed to fail job");

        let failed_job = &all_jobs(&db).await.expect("failed to fetch all jobs")[0];

        assert_eq!(failed_job.failed_attempts, 1);
        assert_eq!(failed_job.status, PostgresJobStatus::Queued);
        assert!(failed_job.updated_at > job.updated_at);
    }
}
