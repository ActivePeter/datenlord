use std::collections::HashSet;

use super::etcd_delegate::EtcdDelegate;

/// Distributed rwlock
#[derive(PartialEq, Eq)]
enum DistRwLockType{
    /// read lock
    RLock,
    /// write lock
    WLock,
}

/// Serialize to lock value
struct RwLockValue{
    /// type of lock
    locktype: DistRwLockType, 
    /// maybe node ip or node id, to verify the ownership of lock
    tags_of_nodes: HashSet<String> 
}

async fn renew_lease(){

}

async fn wait_release(){

}

/// Lock a rwlock
#[inline]
pub async fn rw_lock(
    etcd: &EtcdDelegate, 
    name: &[u8], 
    locktype: DistRwLockType,
    timeout: Duration,
    tag_of_local_node: &str, // mark node tag
){
    // todo1 fairness
    // todo2 timeout of different read lock
    loop{
        // fix: we need a version to make sure the update is safe.
        let res=etcd.write_new_kv_no_panic(name, &RwLockValue{
            locktype,
            tags_of_nodes: {
                let v=Vec::new();
                v.push(tag_of_local_node.to_owned());
                v
            },
        }, Some(timeout)).await?;

        match res{
            // lock exists
            Some(res) => {
                if res.locktype==DistRwLockType::RLock && locktype==DistRwLockType::RLock{
                    // remote: r | current: r
                    // check if the node exist
                    // 1. if node exists in the set, renew the lease (already hold)
                    // 2. if node doesn't exist, add it into the set and renew the lease
                    res.tags_of_nodes.insert(tag_of_local_node.to_owned());
                }else if res.locktype==DistRwLockType::RLock && locktype==DistRwLockType::RLock{
                    // remote: w | current: w
                    // 1. if same node, renew the lease
                    // 2. else, wait release
                }else{
                    // remote: r | current: w
                    // wait release
                    // remote: w | current: r
                    // wait release
                }
            },
            // lock successfully
            None => {
                // todo: log    
            },
        }
    }
}

#[inline]
pub async fn rw_unlock(
    client: &etcd_client::Client, 
    name: &[u8], 
){

}
