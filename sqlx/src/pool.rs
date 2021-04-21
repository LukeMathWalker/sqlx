use crate::pool::connection::{Idle, Pooled};
use crate::pool::options::PoolOptions;
use crate::pool::shared::{SharedPool, TryAcquireResult};
use crate::pool::wait_list::WaitList;
use crate::{Connect, Connection, DefaultRuntime, Runtime};
use crossbeam_queue::ArrayQueue;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod connection;
mod options;
mod shared;
mod wait_list;

pub struct Pool<Rt: Runtime, C: Connection<Rt>> {
    shared: Arc<SharedPool<Rt, C>>,
}

impl<Rt: Runtime, C: Connection<Rt>> Pool<Rt, C> {
    pub fn new(uri: &str) -> crate::Result<Self> {
        Self::builder().build(uri)
    }

    pub fn new_with(connect_options: <C as Connect<Rt>>::Options) -> Self {
        Self::builder().build_with(connect_options)
    }

    pub fn builder() -> PoolOptions<Rt, C> {
        PoolOptions::new()
    }
}

#[cfg(feature = "async")]
impl<Rt: crate::Async, C: Connection<Rt>> Pool<Rt, C> {
    pub async fn connect(uri: &str) -> crate::Result<Self> {
        Self::builder().connect(uri).await
    }

    pub async fn connect_with(connect_options: <C as Connect<Rt>>::Options) -> crate::Result<Self> {
        Self::builder().connect_with(connect_options).await
    }

    pub async fn acquire(&self) -> crate::Result<Pooled<Rt, C>> {
        if let Some(timeout) = self.shared.pool_options.acquire_timeout {
            self.acquire_timeout(timeout).await
        } else {
            self.acquire_inner().await
        }
    }

    pub async fn acquire_timeout(&self, timeout: Duration) -> crate::Result<Pooled<Rt, C>> {
        Rt::timeout_async(timeout, self.acquire_inner())
            .await
            .ok_or(crate::Error::AcquireTimedOut)?
    }

    async fn acquire_inner(&self) -> crate::Result<Pooled<Rt, C>> {
        let mut acquire_permit = None;

        loop {
            match self.shared.try_acquire(acquire_permit.take()) {
                TryAcquireResult::Acquired(mut conn) => {
                    match self.shared.on_acquire_async(&mut conn) {
                        Ok(()) => return Ok(conn.attach(&self.shared)),
                        Err(e) => {
                            log::info!("error from before_acquire: {:?}", e);
                        }
                    }
                }
                TryAcquireResult::Connect(permit) => self.shared.connect_async(permit).await,
                TryAcquireResult::Wait => {
                    acquire_permit = Some(self.shared.wait_async().await);
                }
                TryAcquireResult::PoolClosed => Err(crate::Error::Closed),
            }
        }
    }
}
