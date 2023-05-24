//! Wrap the basic distribute rwlock in common::rwlock
//!  Currently it's mainly for locking the file when open a file
//!  Auto renew the lease when the lock is about to release

use crate::common::error::DatenLordError;
use crate::{
    async_fuse::memfs::metadata::MetaData,
    common::{
        dist_rwlock::{self, DistRwLockType},
        error::DatenLordResult,
        etcd_delegate::EtcdDelegate,
    },
};
use parking_lot::RwLock;
use tokio::task::JoinHandle;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
    time::Duration,
};
use thiserror::Error;
use tokio::sync::mpsc::Sender;

/// `DatenLord` error code
#[derive(Error, Debug)]
pub enum FsDistRwLockErr {
    /// Error happens when operation after system closed.
    #[error(
        "FsDistRwLockErr, system is stopped when operation: {}",
        when_operation
    )]
    SystemStopped {
        /// Describe the related operation when the error happends
        when_operation: String,
    },
}

/// The manager to lock,unlock distribute rwlock and renew the lease of using lock
#[allow(missing_debug_implementations)]
pub struct DistRwLockManager<M: MetaData + Send + Sync + 'static> {
    locked: RwLock<DistRwLockManagerLocked>,
    etcd: Arc<EtcdDelegate>,
}
struct DistRwLockManagerLocked {
    /// lockname - timeout
    lock_timeout_cancels: HashMap<String, Arc<AtomicBool>>,
    /// sender for timer to notify the main task to renew the lease
    ///  `stop_task` will set it to None, when all senders being dropped, the main task will end
    sender_to_maintask: Option<Sender<(String, DistRwLockType)>>,
}
impl<M: MetaData + Send + Sync + 'static> DistRwLockManager<M> {
    /// new `DistRwLockManager`
    pub fn new(etcd: Arc<EtcdDelegate>) -> (JoinHandle<()>, Arc<DistRwLockManager<M>>) {
        //init a main loop to renew the lock lease
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(String, DistRwLockType)>(10);
        let rwman: Arc<DistRwLockManager<M>> = Arc::new(DistRwLockManager {
            locked: RwLock::new(DistRwLockManagerLocked {
                lock_timeout_cancels: HashMap::default(),
                sender_to_maintask: Some(tx),
            }),
            etcd,
        });
        let rwmanclone = Arc::clone(&rwman);
        // renew lock task, when the lock lease is about to expire
        let joiner = tokio::spawn(async move {
            while let Some((lockname, locktype)) = rx.recv().await {
                if let Err(err) = rwman.lock(&lockname, locktype).await {
                    log::warn!("renew lock failed, err:{}", err);
                    break;
                }
            }
            log::debug!("renew lock task is closing");
        });
        (joiner, rwmanclone)
    }
    fn set_timeout(
        &self,
        name: &str,
        locktype: DistRwLockType,
        renew_timeout: Duration,
    ) -> DatenLordResult<()> {
        let tx = self.locked.read().sender_to_maintask.clone();
        let namestring = name.to_owned();
        let timeout_cancel = Arc::new(AtomicBool::new(false));
        if let Some(tx) = tx {
            {
                let timeout_cancel = Arc::clone(&timeout_cancel);
                self.locked
                    .write()
                    .lock_timeout_cancels
                    .entry(name.to_owned())
                    .and_modify(|old_timeout_cancel| {
                        // cancel prev timeout
                        old_timeout_cancel.store(true, Ordering::SeqCst);
                        *old_timeout_cancel = Arc::clone(&timeout_cancel);
                    })
                    .or_insert_with(|| timeout_cancel);
            }
            let _ = tokio::time::timeout(renew_timeout, async move {
                if !timeout_cancel.load(Ordering::SeqCst) {
                    tx.send((namestring, locktype)).await.unwrap_or_else(|err| {
                        panic!(
                            "async renew lock task should be always working, err:{}",
                            err
                        );
                    });
                }
            });
            Ok(())
        } else {
            log::debug!("dist rwlock system is closed");
            Err(DatenLordError::FsDistRwLockErr {
                source: FsDistRwLockErr::SystemStopped {
                    when_operation: "set_timeout".to_owned(),
                },
                context: vec![],
            })
        }
    }
    /// Lock dist rwlock
    pub async fn lock(
        &self,
        metadata: &M,
        name: &str,
        locktype: DistRwLockType,
    ) -> DatenLordResult<()> {
        let default_timeout = Duration::from_secs(10);
        let default_renew_timeout = Duration::from_secs(8);
        // lock
        dist_rwlock::rw_lock(
            &self.etcd,
            name,
            locktype,
            default_timeout,
            metadata.node_id(),
        )
        .await?;
        // set timeout
        self.set_timeout(name, locktype, default_renew_timeout)
    }
    /// Try lock dist rwlock
    pub async fn try_lock(
        &self,
        metadata: &M,
        name: &str,
        locktype: DistRwLockType,
    ) -> DatenLordResult<bool> {
        let default_timeout = Duration::from_secs(10);
        let default_renew_timeout = Duration::from_secs(8);
        // lock
        if dist_rwlock::rw_try_lock(
            &self.etcd,
            name,
            locktype,
            default_timeout,
            metadata.node_id(),
        )
        .await?
        {
            // set timeout
            self.set_timeout(name, locktype, default_renew_timeout)
        } else {
            Ok(false)
        }
    }
    /// Unlock dist rwlock
    pub async fn unlock(&self, name: &str, metadata: &M) -> DatenLordResult<()> {
        dist_rwlock::rw_unlock(&self.etcd, name, metadata.node_id()).await
    }
    /// Stop the async task
    pub fn stop_task(&self) {
        let mut locked = self.locked.write();
        for (_lockname, cancel) in &mut locked.lock_timeout_cancels {
            cancel.store(true, Ordering::SeqCst);
        }
        locked.sender_to_maintask.take();
    }
}
