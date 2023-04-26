use std::{collections::HashSet, time::Duration};
use super::{etcd_delegate::{EtcdDelegate, KVVersion}, error::DatenLordResult};
use serde::{Deserialize, Serialize};

/// timeout of the dist lock kv
const DIST_LOCK_TIMEOUT_SEC:u64 = 60;

/// Distributed rwlock
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum DistRwLockType{
    /// read lock
    RLock,
    /// write lock
    WLock,
}

/// Serialize to lock value
#[derive(Serialize, Deserialize, Debug, Clone)]
struct RwLockValue{
    /// type of lock
    locktype: DistRwLockType, 
    /// maybe node ip or node id, to verify the ownership of lock
    tags_of_nodes: HashSet<String> 
}


/// renew the lease for continue use the lock key.
/// 
/// * `version` - Version of the lock need to be updated
#[inline]
async fn renew_lease(etcd: &EtcdDelegate, key: &str, value:RwLockValue, fail_ctx_info: &str, version: KVVersion) -> DatenLordResult<()> {
    match etcd.write_or_update_kv_with_timeout(key, &value, Duration::from_secs(DIST_LOCK_TIMEOUT_SEC)).await{
        Ok(_) => Ok(()),
        Err(err) => {
            log::warn!("renew_lease failed,fail_ctxinfo:{fail_ctx_info},err:{err}");
            Err(err)
        },
    }
}

#[inline]
async fn wait_release(etcd: &EtcdDelegate, key: &str, fail_ctx_info: &str)->DatenLordResult<()>{
    match etcd.wait_key_delete(key).await{
        Ok(_) => Ok(()),
        Err(err) => {
            log::warn!("wait_release failed,fail_ctxinfo:{fail_ctx_info},err:{err}");
            Err(err)
        },
    }
}

/// Lock a rwlock
#[inline]
pub async fn rw_lock(
    etcd: &EtcdDelegate, 
    name: &str, 
    locktype: DistRwLockType,
    timeout: Duration,
    tag_of_local_node: &str, // mark node tag
)->DatenLordResult<()>{
    // todo1 fairness
    // todo2 timeout of different read lock 
    //  It's ok because we only care for the last read lock to be release.
    loop{
        // fix: we need a version to make sure the update is safe.
        let res=etcd.write_new_kv_no_panic(name, &RwLockValue{
            locktype: locktype.clone(),
            tags_of_nodes: {
                let mut s=HashSet::new();
                s.insert(tag_of_local_node.to_owned());
                s
            },
        }, Some(timeout)).await?;

        match res{
            // lock exists
            Some((mut res, version)) => {
                if res.locktype==DistRwLockType::RLock && locktype==DistRwLockType::RLock{
                    // remote: r | current: r
                    // check if the node exist
                    // 1. if node exists in the set, renew the lease (already hold)
                    // 2. if node doesn't exist, add it into the set and renew the lease
                    res.tags_of_nodes.insert(tag_of_local_node.to_owned());
                    // must todo:: when there's conflict, we should offer the version and use the transaction.
                    //  make sure the operated data is the version we got.
                    renew_lease(etcd, name, res, "renew read lock", version).await?;
                    return Ok(());
                }else if res.locktype==DistRwLockType::WLock && locktype==DistRwLockType::WLock{
                    // remote: w | current: w
                    // 1. if same node, renew the lease
                    if res.tags_of_nodes.contains(tag_of_local_node){
                        renew_lease(etcd, name, res, "renew write lock", version).await?;
                        return Ok(());
                    }
                    // 2. else, wait release
                    else{
                        wait_release(etcd, name, "need write lock, wait for write lock").await?;
                    }
                }else{
                    // remote: r | current: w
                    // wait release
                    // remote: w | current: r
                    // wait release
                    wait_release(etcd, name, "different lock type, wait for release").await?;
                }
            },
            // lock successfully
            None => {
                return Ok(())
            },
        }
    }
}

#[inline]
pub async fn rw_unlock(
    etcd: &EtcdDelegate, 
    name: &str, 
    tag_of_local_node: &str, // mark node tag
) -> DatenLordResult<()>{
    let res=etcd.get_at_most_one_value::<RwLockValue,&str>(name).await;
    match res{
        Ok(res) => {
            if let Some((mut lockinfo, version))=res{
                if lockinfo.tags_of_nodes.remove(tag_of_local_node){
                    // match 
                    // must todo: these two operations must be atomic (use transaction to make sure the version)
                    //  if the transaction failed, retry will be needed.
                    if lockinfo.tags_of_nodes.len()==0{
                        // remove the key
                    }else{
                        // update the value
                    }
                    Ok(())
                }else{
                    log::debug!("try unlock, but this node does'nt hold the lock, lock key:{name}");
                    Ok(())
                }
            }else{
                log::debug!("try unlock, but this node does'nt hold the lock, lock key:{name}");
                Ok(())
            }
        },
        Err(err) => {
            log::warn!("etcd error when unlock, err:{err}");
            Err(err)
        },
    }
}
