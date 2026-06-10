use anyhow::{self as ah, Context as _};
use tokio::task::{JoinHandle, spawn};

pub enum Deferred<R>
where
    R: Send + 'static,
{
    Finished(R),
    Pending(Option<JoinHandle<R>>),
}

impl<R> Deferred<R>
where
    R: Send + 'static,
{
    pub fn new(fut: impl Future<Output = R> + Send + 'static) -> Self {
        Deferred::Pending(Some(spawn(fut)))
    }

    pub async fn as_mut(&mut self) -> ah::Result<&mut R> {
        match self {
            Self::Finished(r) => Ok(r),
            Self::Pending(f) => {
                let r = f
                    .take()
                    .context("Future already taken")?
                    .await
                    .context("Failed to join Deferred-task")?;
                *self = Self::Finished(r);
                match self {
                    Self::Finished(r) => Ok(r),
                    Self::Pending(_) => unreachable!(),
                }
            }
        }
    }

    #[allow(dead_code)]
    pub fn as_ref(&self) -> Option<&R> {
        match self {
            Self::Finished(r) => Some(r),
            Self::Pending(_) => None,
        }
    }
}
