use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use uuid::Uuid;

#[async_trait::async_trait]
pub trait Queue: Send + Sync + Debug {
    async fn push(
        &self,
        job: Box<dyn TaskParams>,
        scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), crate::Error>;
    /// pull fetches at most `number_of_jobs` from the queue.
    async fn pull(&self, number_of_jobs: u32) -> Result<Vec<Job>, crate::Error>;
    async fn delete_job(&self, job_id: Uuid) -> Result<(), crate::Error>;
    async fn fail_job(&self, job_id: Uuid) -> Result<(), crate::Error>;
    async fn clear(&self) -> Result<(), crate::Error>;
}

pub trait TaskParams: Debug + Serialize {}

#[async_trait::async_trait]
pub trait Task {
    type Params: TaskParams;

    // async fn run(&self) -> Result<(), crate::Error>;
    // fn name(&self) -> &'static str;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub message: Box<dyn TaskParams>,
}
