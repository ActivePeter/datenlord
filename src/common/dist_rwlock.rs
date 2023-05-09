use super::{
    error::DatenLordResult,
    etcd_delegate::{EtcdDelegate, KVVersion},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, time::Duration};

/// timeout of the dist lock kv
const DIST_LOCK_TIMEOUT_SEC: u64 = 60;

/// Distributed rwlock
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
pub enum DistRwLockType {
    /// read lock
    RLock,
    /// write lock
    WLock,
}

/// Serialize to lock value
#[derive(Serialize, Deserialize, Debug, Clone)]
struct RwLockValue {
    /// type of lock
    locktype: DistRwLockType,
    /// maybe node ip or node id, to verify the ownership of lock
    tags_of_nodes: HashSet<String>,
}

/// Renew the lease for continue use the lock key.
///
/// - `version` - Previous version of the lock need to be updated
#[inline]
async fn renew_lease(
    etcd: &EtcdDelegate,
    key: &str,
    value: RwLockValue,
    fail_ctx_info: &str,
    version: KVVersion,
) -> DatenLordResult<bool> {
    match etcd
        .write_or_update_kv_with_version(
            key,
            &value,
            version,
            Some(Duration::from_secs(DIST_LOCK_TIMEOUT_SEC)),
        )
        .await
    {
        Ok(res) => {
            if res.is_some() {
                Ok(false)
            } else {
                Ok(true)
            }
        }
        Err(err) => {
            log::warn!("renew_lease failed,fail_ctxinfo:{fail_ctx_info},err:{err}");
            Err(err)
        }
    }
}

/// update lock info
#[inline]
async fn update_lock(
    etcd: &EtcdDelegate,
    key: &str,
    value: RwLockValue,
    fail_ctx_info: &str,
    version: KVVersion,
) -> DatenLordResult<bool> {
    match etcd
        .write_or_update_kv_with_version(key, &value, version, None)
        .await
    {
        Ok(res) => {
            if res.is_some() {
                Ok(false)
            } else {
                Ok(true)
            }
        }
        Err(err) => {
            log::warn!("update_lock failed,fail_ctxinfo:{fail_ctx_info},err:{err}");
            Err(err)
        }
    }
}

#[inline]
async fn wait_release(etcd: &EtcdDelegate, key: &str, fail_ctx_info: &str) -> DatenLordResult<()> {
    match etcd.wait_key_delete(key).await {
        Ok(_) => Ok(()),
        Err(err) => {
            log::warn!("wait_release failed,fail_ctxinfo:{fail_ctx_info},err:{err}");
            Err(err)
        }
    }
}

#[inline]
async fn remove_key(etcd: &EtcdDelegate, key: &str) -> DatenLordResult<()> {
    match etcd.delete_one_value::<RwLockValue>(key).await {
        Ok(_) => Ok(()),
        Err(err) => Err(err),
    }
}


// todo 
//  lock timeout log

/// Lock a rwlock
///  if txn failed, will retry.
///  if kv error occured, will return the error directly
#[inline]
pub async fn rw_lock(
    etcd: &EtcdDelegate,
    name: &str,
    locktype: DistRwLockType,
    timeout: Duration,
    tag_of_local_node: &str, // mark node tag
) -> DatenLordResult<()> {
    // todo1 fairness
    // todo2 timeout of different read lock
    let mut failtime = 0;
    //  It's ok because we only care for the last read lock to be release.
    loop {
        // fix: we need a version to make sure the update is safe.
        let res = etcd
            .write_new_kv_no_panic(
                name,
                &RwLockValue {
                    locktype: locktype.clone(),
                    tags_of_nodes: {
                        let mut s = HashSet::new();
                        s.insert(tag_of_local_node.to_owned());
                        s
                    },
                },
                Some(timeout),
            )
            .await?;

        match res {
            // lock exists
            Some((mut res, version)) => {
                if res.locktype == DistRwLockType::RLock && locktype == DistRwLockType::RLock {
                    // remote: r | current: r
                    // check if the node exist
                    // 1. if node exists in the set, renew the lease (already hold)
                    // 2. if node doesn't exist, add it into the set and renew the lease
                    res.tags_of_nodes.insert(tag_of_local_node.to_owned());
                    // must todo:: when there's conflict, we should offer the version and use the transaction.
                    //  make sure the operated data is the version we got.
                    if !renew_lease(etcd, name, res, "renew read lock", version).await? {
                        failtime += 1;
                        log::debug!("etcd renew_lease txn failed on node {tag_of_local_node}, will retry, fail time: {failtime}");
                        continue;
                    }

                    return Ok(());
                } else if res.locktype == DistRwLockType::WLock && locktype == DistRwLockType::WLock
                {
                    // remote: w | current: w
                    // 1. if same node, renew the lease
                    if res.tags_of_nodes.contains(tag_of_local_node) {
                        if !renew_lease(etcd, name, res, "renew write lock", version).await? {
                            failtime += 1;
                            log::debug!("etcd renew_lease txn failed on node {tag_of_local_node}, will retry, fail time: {failtime}");
                            continue;
                        }
                        return Ok(());
                    }
                    // 2. else, wait release
                    else {
                        wait_release(etcd, name, "need write lock, wait for write lock").await?;
                    }
                } else {
                    // remote: r | current: w
                    // wait release
                    // remote: w | current: r
                    // wait release
                    wait_release(etcd, name, "different lock type, wait for release").await?;
                }
            }
            // lock successfully
            None => return Ok(()),
        }
    }
}

/// Unlock rwlock
///  if txn failed, will retry.
///  if kv error occured, will return the error directly
#[inline]
pub async fn rw_unlock(
    etcd: &EtcdDelegate,
    name: &str,
    tag_of_local_node: &str, // mark node tag
) -> DatenLordResult<()> {
    let mut failtime = 0;
    loop {
        let res = etcd.get_at_most_one_value::<RwLockValue, &str>(name).await;
        match res {
            Ok(res) => {
                if let Some((mut lockinfo, version)) = res {
                    if lockinfo.tags_of_nodes.remove(tag_of_local_node) {
                        // These two operations must be atomic (use transaction to make sure the version)
                        //  if the transaction failed, retry will be needed.
                        if lockinfo.tags_of_nodes.len() == 0 {
                            // remove the key
                            return remove_key(etcd, name).await;
                        } else {
                            // update the value
                            if update_lock(
                                etcd,
                                name,
                                lockinfo,
                                "update lock nodes when unlock failed",
                                version,
                            )
                            .await?
                            {
                                return Ok(());
                            } else {
                                failtime += 1;
                                log::debug!(
                                    "update_lock txn failed, fail time: {failtime}, retry unlock"
                                );
                                continue;
                            }
                        }
                    } else {
                        log::debug!(
                            "try unlock, but this node does'nt hold the lock, lock key:{name}"
                        );
                        return Ok(());
                    }
                } else {
                    log::debug!("try unlock, but this node does'nt hold the lock, lock key:{name}");
                    return Ok(());
                }
            }
            Err(err) => {
                log::warn!("etcd error when unlock, err:{err}");
                return Err(err);
            }
        }
    }
}

// test todo
//  single node read write
//  single node read
//  single node write
//  multi node read
//  multi node write
//  multi node read write
//  etcd error when working

#[cfg(test)]
mod test {
    use std::time::{SystemTime,UNIX_EPOCH};

    use super::*;
    
    const ETCD_ADDRESS: &str = "localhost:2379";

    fn timestamp()->Duration{
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|err| panic!("Time went backwards, err:{err}"))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_etcd() {
        let mut addrs=vec![];
        addrs.push(ETCD_ADDRESS.to_owned());
        let etcd=EtcdDelegate::new(addrs).await;
        match etcd{
            Ok(etcd)=>{
                let testk="test_k";
                if let Err(err)=etcd.write_or_update_kv(testk, &testk.to_owned()).await{
                    panic!("write etcd kv failed with err: {err}");
                }
                let deleted=etcd.delete_exact_one_value::<String>(testk).await;
                match deleted{
                    Ok((res,_))=>{
                        assert_eq!(&res,testk);
                    }
                    Err(err)=>{
                        panic!("delete test key failed, err:{err}");
                    }
                }
            }
            Err(err)=>{
                panic!("failed to connect etcd, err:{err}");
            }
        }
    }

    #[allow(clippy::unwrap_used)]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_rwlock() {
        let mut addrs=vec![];
        addrs.push(ETCD_ADDRESS.to_owned());
        let etcd=EtcdDelegate::new(addrs).await;
        match etcd{
            Ok(etcd)=>{
                {
                    // There's no conflict between different lock 
                    let begin=timestamp();
                    rw_lock(&etcd, "lock", DistRwLockType::RLock, Duration::from_secs(1), "node_tag").await.unwrap();
                    rw_lock(&etcd, "lock2", DistRwLockType::RLock, Duration::from_secs(1), "node_tag").await.unwrap();
                    let end=timestamp();
                    assert!((end-begin).as_millis()<10);
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                {
                   //same node update same lock 
                   let begin=timestamp();
                   rw_lock(&etcd, "lock", DistRwLockType::RLock, Duration::from_secs(1), "node_tag").await.unwrap();
                   rw_lock(&etcd, "lock", DistRwLockType::RLock, Duration::from_secs(1), "node_tag").await.unwrap();
                   let end=timestamp();
                   assert!((end-begin).as_millis()<10);
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                {
                    //different nodes get same rlock 
                    let begin=timestamp();
                    rw_lock(&etcd, "lock", DistRwLockType::RLock, Duration::from_secs(1), "node_tag").await.unwrap();
                    rw_lock(&etcd, "lock", DistRwLockType::RLock, Duration::from_secs(1), "node_tag2").await.unwrap();
                    let end=timestamp();
                    assert!((end-begin).as_millis()<10);
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                {
                    // There's conflict between different lock type
                    let begin=timestamp();
                    rw_lock(&etcd, "lock", DistRwLockType::WLock, Duration::from_secs(1), "node_tag").await.unwrap();
                    rw_lock(&etcd, "lock", DistRwLockType::RLock, Duration::from_secs(1), "node_tag").await.unwrap();
                    let end=timestamp();
                    assert!((end-begin).as_millis()>900);
                 }
                 tokio::time::sleep(Duration::from_secs(1)).await;
                 {
                    // different node get different type lock
                    let begin=timestamp();
                    rw_lock(&etcd, "lock", DistRwLockType::WLock, Duration::from_secs(1), "node_tag1").await.unwrap();
                    rw_lock(&etcd, "lock", DistRwLockType::RLock, Duration::from_secs(1), "node_tag").await.unwrap();
                    let end=timestamp();
                    assert!((end-begin).as_millis()>900);
                 }
                 tokio::time::sleep(Duration::from_secs(1)).await;
                 {
                    // unlock
                    let begin=timestamp();
                    rw_lock(&etcd, "lock", DistRwLockType::WLock, Duration::from_secs(1), "node_tag").await.unwrap();
                    rw_unlock(&etcd, "lock", "node_tag").await.unwrap();
                    rw_lock(&etcd, "lock", DistRwLockType::WLock, Duration::from_secs(1), "node_tag").await.unwrap();
                    let end=timestamp();
                    assert!((end-begin).as_millis()<10);
                 }
                 tokio::time::sleep(Duration::from_secs(1)).await;
                 {
                    // unlock by other node is not ok 
                    let begin=timestamp();
                    rw_lock(&etcd, "lock", DistRwLockType::WLock, Duration::from_secs(1), "node_tag").await.unwrap();
                    rw_unlock(&etcd, "lock", "node_tag1").await.unwrap();
                    rw_lock(&etcd, "lock", DistRwLockType::WLock, Duration::from_secs(1), "node_tag").await.unwrap();
                    let end=timestamp();
                    assert!((end-begin).as_millis()>900);
                 }
                 tokio::time::sleep(Duration::from_secs(1)).await;
                 // todo: add test about etcd crash
            }
            Err(err)=>{
                panic!("failed to connect etcd, err:{err}");
            }
        }
    }
}
