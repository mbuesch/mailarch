use anyhow::{self as ah, Context as _};
use tokio::task::{JoinHandle, spawn};

/// Spawn a tokio task and join it later. Cache the result of the task.
#[derive(Debug)]
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
    /// Spawn a future as a Deferred task.
    pub fn spawn<F>(f: F) -> Self
    where
        F: Future<Output = R> + Send + 'static,
    {
        Deferred::Pending(Some(spawn(f)))
    }

    /// Join the deferred task and return its mutable result.
    ///
    /// If the task has not finished yet, this will await the task and store the result for future calls.
    /// If the task has already finished, return the result immediately.
    pub async fn join(&mut self) -> ah::Result<&mut R> {
        match self {
            Self::Finished(r) => Ok(r),
            Self::Pending(f) => {
                let r = f
                    .take()
                    .context("Future already taken (should never happen)")?
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

    /// Get a mutable reference to the result if the deferred task has already finished.
    ///
    /// If the task is still pending, return None.
    /// This does not join the task and will not change the state of the deferred future.
    #[allow(dead_code)]
    pub fn as_mut(&mut self) -> Option<&mut R> {
        match self {
            Self::Finished(r) => Some(r),
            Self::Pending(_) => None,
        }
    }

    /// Get a reference to the result if the deferred task has already finished.
    ///
    /// If the task is still pending, return None.
    /// This does not join the task and will not change the state of the deferred future.
    #[allow(dead_code)]
    pub fn as_ref(&self) -> Option<&R> {
        match self {
            Self::Finished(r) => Some(r),
            Self::Pending(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn test_deferred() {
        let b = Arc::new((Barrier::new(2), Barrier::new(2), Barrier::new(2)));
        let cnt = Arc::new(AtomicU32::new(0));
        let mut d = Deferred::spawn({
            let b = Arc::clone(&b);
            let cnt = Arc::clone(&cnt);
            async move {
                b.0.wait().await;
                cnt.store(1, Ordering::SeqCst);
                b.1.wait().await;
                b.2.wait().await;
                cnt.store(2, Ordering::SeqCst);
                42
            }
        });
        assert_eq!(cnt.load(Ordering::SeqCst), 0);
        b.0.wait().await;
        b.1.wait().await;
        assert_eq!(cnt.load(Ordering::SeqCst), 1);
        assert!(d.as_mut().is_none());
        assert!(d.as_ref().is_none());
        b.2.wait().await;
        assert_eq!(*d.join().await.unwrap(), 42);
        assert_eq!(cnt.load(Ordering::SeqCst), 2);
        assert_eq!(*d.as_mut().unwrap(), 42);
        assert_eq!(*d.as_ref().unwrap(), 42);
    }
}
