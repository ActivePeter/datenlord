use etcd_client::TxnCmp;

use super::etcd_delegate::EtcdDelegate;

/// Distributed rwlock
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
    tags_of_local_nodes: Vec<String> 
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
    
    let res=etcd.write_new_kv_no_panic(name, &RwLockValue{
        locktype,
        tags_of_local_nodes: {
            let v=Vec::new();
            v.push(tag_of_local_node.to_owned());
            v
        },
    }, Some(timeout)).await?;
    
    match res{
        // already has lock
        Some(res) => {
            // check if the node exist
            // 1.if node exist 
        },
        // lock successfully
        None => {
            // todo: log    
        },
    }
}

#[inline]
pub async fn rw_unlock(
    client: &etcd_client::Client, 
    name: &[u8], 
){

}
