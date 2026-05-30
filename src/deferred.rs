pub enum Deferred<R, F>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    Finished(R),
    Pending(Option<F>),
}

impl<R, F> Deferred<R, F>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    pub fn new(fut: F) -> Self {
        Deferred::Pending(Some(fut))
    }

    pub async fn as_mut(&mut self) -> &mut R {
        match self {
            Self::Finished(r) => r,
            Self::Pending(f) => {
                let r = f.take().expect("Future already taken").await;
                *self = Self::Finished(r);
                match self {
                    Self::Finished(r) => r,
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
