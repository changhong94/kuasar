// SPDX-License-Identifier: Apache-2.0
//! Reject new lifecycle operations while a fork barrier drains existing ones.
use std::{sync::Arc, time::Duration};

use containerd_shim::protos::{api::*, shim_async::Task};
use tokio::sync::{mpsc::Receiver, watch, Mutex, OwnedRwLockWriteGuard, RwLock};
use ttrpc::{r#async::TtrpcContext, Result};

pub struct ForkGate {
    pub lock: Arc<RwLock<()>>,
    lease: Mutex<Option<(String, OwnedRwLockWriteGuard<()>)>>,
    interrupt_events: watch::Sender<bool>,
}
impl Default for ForkGate {
    fn default() -> Self {
        Self {
            lock: Arc::new(RwLock::new(())),
            lease: Mutex::new(None),
            interrupt_events: watch::channel(false).0,
        }
    }
}

// Cancellation or timeout of a freeze must not leave event polling disabled.
// Once committed, the token-owned lease keeps the signal set until release.
struct PendingFreeze<'a> {
    signal: &'a watch::Sender<bool>,
    committed: bool,
}
impl Drop for PendingFreeze<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.signal.send_replace(false);
        }
    }
}

fn busy(message: &str) -> ttrpc::Error {
    ttrpc::Error::RpcStatus(ttrpc::get_status(ttrpc::Code::FAILED_PRECONDITION, message))
}
impl ForkGate {
    pub fn enter(&self) -> Result<tokio::sync::RwLockReadGuard<'_, ()>> {
        self.lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))
    }

    pub async fn wait_event<T>(&self, rx: &Mutex<Receiver<T>>) -> Result<Option<T>> {
        // Subscribe before taking the read lock. A retained watch value also
        // covers a freeze requested before the cancellation future is polled.
        let mut interrupt = self.interrupt_events.subscribe();
        let _guard = self.enter()?;
        tokio::select! {
            biased;
            _ = interrupt.wait_for(|requested| *requested) => {
                Err(busy("event poll interrupted for VM fork"))
            }
            result = tokio::time::timeout(Duration::from_millis(200), async {
                rx.lock().await.recv().await
            }) => {
                result.map_err(|_| ttrpc::Error::RpcStatus(ttrpc::get_status(
                    ttrpc::Code::DEADLINE_EXCEEDED,
                    "event poll timeout",
                )))
            }
        }
        // recv() and Mutex::lock() are cancellation-safe. On interruption the
        // queue consumer is dropped before _guard, so the write barrier still
        // acknowledges its exit rather than merely requesting cancellation.
    }

    pub async fn control(&self, token: &str, release: bool) -> Result<()> {
        if token.is_empty() {
            return Err(busy("fork token must not be empty"));
        }
        let mut lease = self.lease.lock().await;
        if let Some((owner, _)) = lease.as_ref() {
            if owner != token {
                return Err(busy("another fork owns the gate"));
            }
            if release {
                self.interrupt_events.send_replace(false);
                lease.take();
            }
            return Ok(());
        }
        if !release {
            let mut pending = PendingFreeze {
                signal: &self.interrupt_events,
                committed: false,
            };
            self.interrupt_events.send_replace(true);
            let guard =
                tokio::time::timeout(Duration::from_secs(10), self.lock.clone().write_owned())
                    .await
                    .map_err(|_| busy("timed out draining Task requests"))?;
            *lease = Some((token.to_owned(), guard));
            pending.committed = true;
        }
        Ok(())
    }
}
pub struct ForkableTask<T> {
    pub inner: T,
    pub gate: Arc<ForkGate>,
}
#[async_trait::async_trait]
impl<T: Task + Send + Sync> Task for ForkableTask<T> {
    async fn state(&self, ctx: &TtrpcContext, req: StateRequest) -> Result<StateResponse> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.state(ctx, req).await
    }
    async fn create(
        &self,
        ctx: &TtrpcContext,
        req: CreateTaskRequest,
    ) -> Result<CreateTaskResponse> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.create(ctx, req).await
    }
    async fn start(&self, ctx: &TtrpcContext, req: StartRequest) -> Result<StartResponse> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.start(ctx, req).await
    }
    async fn delete(&self, ctx: &TtrpcContext, req: DeleteRequest) -> Result<DeleteResponse> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.delete(ctx, req).await
    }
    async fn pids(&self, ctx: &TtrpcContext, req: PidsRequest) -> Result<PidsResponse> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.pids(ctx, req).await
    }
    async fn pause(&self, ctx: &TtrpcContext, req: PauseRequest) -> Result<Empty> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.pause(ctx, req).await
    }
    async fn resume(&self, ctx: &TtrpcContext, req: ResumeRequest) -> Result<Empty> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.resume(ctx, req).await
    }
    async fn checkpoint(&self, ctx: &TtrpcContext, req: CheckpointTaskRequest) -> Result<Empty> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.checkpoint(ctx, req).await
    }
    async fn kill(&self, ctx: &TtrpcContext, req: KillRequest) -> Result<Empty> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.kill(ctx, req).await
    }
    async fn exec(&self, ctx: &TtrpcContext, req: ExecProcessRequest) -> Result<Empty> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.exec(ctx, req).await
    }
    async fn resize_pty(&self, ctx: &TtrpcContext, req: ResizePtyRequest) -> Result<Empty> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.resize_pty(ctx, req).await
    }
    async fn close_io(&self, ctx: &TtrpcContext, req: CloseIORequest) -> Result<Empty> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.close_io(ctx, req).await
    }
    async fn update(&self, ctx: &TtrpcContext, req: UpdateTaskRequest) -> Result<Empty> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.update(ctx, req).await
    }
    async fn wait(&self, ctx: &TtrpcContext, req: WaitRequest) -> Result<WaitResponse> {
        // Wait does not mutate lifecycle state; holding a gate would deadlock fork.
        self.inner.wait(ctx, req).await
    }
    async fn stats(&self, ctx: &TtrpcContext, req: StatsRequest) -> Result<StatsResponse> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.stats(ctx, req).await
    }
    async fn connect(&self, ctx: &TtrpcContext, req: ConnectRequest) -> Result<ConnectResponse> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.connect(ctx, req).await
    }
    async fn shutdown(&self, ctx: &TtrpcContext, req: ShutdownRequest) -> Result<Empty> {
        let _guard = self
            .gate
            .lock
            .try_read()
            .map_err(|_| busy("Task lifecycle is frozen for VM fork"))?;
        self.inner.shutdown(ctx, req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pending<F: std::future::Future>(mut future: std::pin::Pin<&mut F>) {
        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }

    #[tokio::test]
    async fn freeze_interrupts_poll_without_consuming_a_racing_event() {
        let gate = ForkGate::default();
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let rx = Mutex::new(rx);
        let mut poll = Box::pin(gate.wait_event(&rx));
        pending(poll.as_mut()).await;
        assert!(rx.try_lock().is_err());
        let mut freeze = Box::pin(gate.control("owner", false));
        pending(freeze.as_mut()).await;
        tx.send(42).await.unwrap();
        // Both branches are ready. Cancellation must leave the event queued.
        assert!(poll.await.is_err());
        assert!(rx.try_lock().is_ok());
        freeze.await.unwrap();
        assert!(gate.wait_event(&rx).await.is_err());
        gate.control("owner", true).await.unwrap();
        assert_eq!(gate.wait_event(&rx).await.unwrap(), Some(42));
    }

    #[tokio::test]
    async fn cancellation_wakes_all_pollers_but_still_drains_lifecycle_work() {
        let gate = ForkGate::default();
        let (_tx, rx) = tokio::sync::mpsc::channel::<u8>(1);
        let rx = Mutex::new(rx);
        let active_task = gate.enter().unwrap();
        let mut first = Box::pin(gate.wait_event(&rx));
        let mut second = Box::pin(gate.wait_event(&rx));
        pending(first.as_mut()).await;
        pending(second.as_mut()).await; // waiting for the receiver mutex
        let mut freeze = Box::pin(gate.control("owner", false));
        pending(freeze.as_mut()).await;
        assert!(first.await.is_err());
        assert!(second.await.is_err());
        pending(freeze.as_mut()).await;
        drop(active_task);
        freeze.await.unwrap();
        gate.control("owner", true).await.unwrap();
    }

    #[tokio::test]
    async fn aborted_freeze_resets_signal_and_allows_a_later_fork() {
        let gate = ForkGate::default();
        let active = gate.enter().unwrap();
        let mut freeze = Box::pin(gate.control("owner", false));
        pending(freeze.as_mut()).await;
        assert!(*gate.interrupt_events.borrow());
        drop(freeze);
        assert!(!*gate.interrupt_events.borrow());
        drop(active);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let rx = Mutex::new(rx);
        tx.send(7).await.unwrap();
        assert_eq!(gate.wait_event(&rx).await.unwrap(), Some(7));
        gate.control("next", false).await.unwrap();
        gate.control("next", true).await.unwrap();
    }

    #[tokio::test]
    async fn retained_signal_covers_subscriber_starting_during_freeze() {
        let gate = ForkGate::default();
        // Model the short interval after signalling and before queuing writer.
        gate.interrupt_events.send_replace(true);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let rx = Mutex::new(rx);
        tx.send(9).await.unwrap();
        assert!(gate.wait_event(&rx).await.is_err());
        gate.interrupt_events.send_replace(false);
        assert_eq!(gate.wait_event(&rx).await.unwrap(), Some(9));
    }

    #[tokio::test]
    async fn ordinary_event_timeout_keeps_the_existing_deadline() {
        let gate = ForkGate::default();
        let (_tx, rx) = tokio::sync::mpsc::channel::<u8>(1);
        let rx = Mutex::new(rx);
        let started = tokio::time::Instant::now();
        let error = gate.wait_event(&rx).await.unwrap_err();
        assert!(started.elapsed() >= Duration::from_millis(200));
        assert!(matches!(error, ttrpc::Error::RpcStatus(status)
            if status.code() == ttrpc::Code::DEADLINE_EXCEEDED));
        assert!(gate.enter().is_ok());
        assert!(rx.try_lock().is_ok());
    }

    #[tokio::test]
    async fn fork_drains_active_requests_and_rejects_new_ones() {
        let gate = Arc::new(ForkGate::default());
        let active = gate.lock.read().await;
        let worker = gate.clone();
        let freeze = tokio::spawn(async move { worker.control("owner", false).await });
        // Tokio's fair RwLock rejects incoming readers once the writer is queued.
        for _ in 0..100 {
            if gate.lock.try_read().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(gate.lock.try_read().is_err());
        assert!(!freeze.is_finished());
        drop(active);
        freeze.await.unwrap().unwrap();
        assert!(gate.lock.try_read().is_err());
        gate.control("owner", true).await.unwrap();
        assert!(gate.lock.try_read().is_ok());
    }

    #[tokio::test]
    async fn fork_token_ownership_and_idempotent_release() {
        let gate = ForkGate::default();
        assert!(gate.control("", false).await.is_err());
        gate.control("one", false).await.unwrap();
        gate.control("one", false).await.unwrap();
        assert!(gate.control("two", false).await.is_err());
        assert!(gate.control("two", true).await.is_err());
        assert!(gate.lock.try_read().is_err());
        gate.control("one", true).await.unwrap();
        gate.control("one", true).await.unwrap();
        gate.control("two", false).await.unwrap();
        gate.control("two", true).await.unwrap();
        assert!(gate.lock.try_read().is_ok());
    }
}
