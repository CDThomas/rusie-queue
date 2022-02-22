use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use uuid::Uuid;

#[async_trait]
pub trait Queue<T: Serialize + DeserializeOwned>: Send + Sync + Debug {
    async fn push(
        &self,
        job: T,
        scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), crate::Error>
    where
        T: 'async_trait;
    /// pull fetches at most `number_of_jobs` from the queue.
    async fn pull(&self, number_of_jobs: u32) -> Result<Vec<Job<T>>, crate::Error>;
    async fn delete_job(&self, job_id: Uuid) -> Result<(), crate::Error>;
    async fn fail_job(&self, job_id: Uuid) -> Result<(), crate::Error>;
    async fn clear(&self) -> Result<(), crate::Error>;
}

#[derive(Debug, Clone)]
pub struct Job<T> {
    pub id: Uuid,
    pub message: T,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Message {
    SendSignInEmail {
        email: String,
        name: String,
        code: String,
    },
    // ...
}
