use crate::async_fuse::memfs::INum;
use crate::common::error::Context;
use crate::common::etcd_delegate::EtcdDelegate;
use log::debug;
use std::collections::HashSet;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;
/// ETCD node id counter key
const ETCD_NODE_ID_COUNTER_KEY: &str = "datenlord_node_id_counter";
/// ETCD node ip and port info
const ETCD_NODE_IP_PORT_PREFIX: &str = "datenlord_node_ip_info_";
/// ETCD volume information lock
const ETCD_VOLUME_INFO_LOCK: &str = "datenlord_volume_info_lock";
/// ETCD volume information prefix
const ETCD_VOLUME_INFO_PREFIX: &str = "datenlord_volume_info_";
/// ETCD file node list lock prefix
const ETCD_FILE_NODE_LIST_LOCK_PREFIX: &str = "datenlord_file_node_list_lock_";
/// ETCD file node list prefix
const ETCD_FILE_NODE_LIST_PREFIX: &str = "datenlord_file_node_list_";
/// ETCD inode number lock
const ETCD_INODE_NUMBER_LOCK: &str = "datenlord_inode_number_lock";
/// ETCD inode number next range
const ETCD_INODE_NEXT_RANGE: &str = "datenlord_inode_number_next_range";
/// ETCD inode number mark prefix
const ETCD_INODE_MARK_PREFIX: &str = "datenlord_inode_mark_";

/// Register current node to etcd.
/// The registered information contains IP.
pub async fn register_node_id(
    etcd_client: &EtcdDelegate,
    node_id: &str,
    node_ipaddr: &str,
    port: &str,
) -> anyhow::Result<()> {
    etcd_client
        .write_or_update_kv(
            format!("{ETCD_NODE_IP_PORT_PREFIX}{node_id}").as_str(),
            &format!("{node_ipaddr}:{port}"),
        )
        .await
        .with_context(|| {
            format!(
                "Update Node Ip address failed, node_id:{node_id}, node_ipaddr: {node_ipaddr}, port: {port}"
            )
        })?;

    Ok(())
}

/// Unregister current node in etcd.
/// The registered information contains IP.
pub async fn unregister_node_id(
    etcd_client: &EtcdDelegate,
    node_id: &str
) -> anyhow::Result<()> {
    etcd_client
        .delete_exact_one_value(
            format!("{ETCD_NODE_IP_PORT_PREFIX}{node_id}").as_str(),
        )
        .await
        .with_context(|| {
            format!(
                "Remove node info in etcd failed, node_id:{node_id}"
            )
        })?;
    Ok(())
}

/// Get ip address and port of a node
pub async fn get_node_ip_and_port(
    etcd_client: Arc<EtcdDelegate>,
    node_id: &str,
) -> anyhow::Result<String> {
    let ip_and_port = etcd_client
        .get_at_most_one_value(format!("{ETCD_NODE_IP_PORT_PREFIX}{node_id}"))
        .await
        .with_context(|| {
            format!("Fail to get Node Ip address and port information, node_id:{node_id}")
        })?;

    if let Some(ip_and_port) = ip_and_port {
        debug!("node {} ip and port is {}", node_id, ip_and_port);
        Ok(ip_and_port)
    } else {
        debug!("node {} missing ip and port information", node_id);
        Err(anyhow::Error::msg(format!(
            "Ip and port is not registered for Node {node_id}"
        )))
    }
}

/// Register volume information, add the volume to `node_id` list mapping
pub async fn register_volume(
    etcd_client: &EtcdDelegate,
    node_id: &str,
    volume_info: &str,
) -> anyhow::Result<()> {
    let lock_key = etcd_client
        .lock(ETCD_VOLUME_INFO_LOCK.as_bytes(), 10)
        .await
        .with_context(|| "lock fail while register volume")?;

    let volume_info_key = format!("{ETCD_VOLUME_INFO_PREFIX}{volume_info}");
    let volume_node_list: Option<Vec<u8>> = etcd_client
        .get_at_most_one_value(volume_info_key.as_str())
        .await
        .with_context(|| format!("get {volume_info_key} from etcd fail"))?;

    /*
    let new_volume_node_list = if let Some(node_list) = volume_node_list {
        let mut node_set: HashSet<String> = bincode::deserialize(node_list.as_slice())
            .unwrap_or_else(|e| {
                panic!(
                    "fail to deserialize node list for volume {:?}, error: {}",
                    volume_info, e
                );
            });
        if !node_set.contains(node_id) {
            node_set.insert(node_id.to_owned());
        }

        node_set
    } else {
        let mut hash = HashSet::new();
        hash.insert(node_id.to_owned());
        hash
    };
    */

    let new_volume_node_list = volume_node_list.map_or_else(
        || {
            let mut hash = HashSet::new();
            hash.insert(node_id.to_owned());
            hash
        },
        |node_list| {
            let mut node_set: HashSet<String> = bincode::deserialize(node_list.as_slice())
                .unwrap_or_else(|e| {
                    panic!("fail to deserialize node list for volume {volume_info:?}, error: {e}");
                });
            if !node_set.contains(node_id) {
                node_set.insert(node_id.to_owned());
            }

            node_set
        },
    );

    let volume_node_list_bin = bincode::serialize(&new_volume_node_list).unwrap_or_else(|e| {
        panic!("fail to serialize node list for volume {volume_info:?}, error: {e}")
    });

    etcd_client
        .write_or_update_kv(volume_info_key.as_str(), &volume_node_list_bin)
        .await
        .with_context(|| {
            format!(
                "Update Volume to Node Id mapping failed, volume:{volume_info}, node id: {node_id}"
            )
        })?;

    etcd_client
        .unlock(lock_key)
        .await
        .with_context(|| "unlock fail while register volume")?;
    Ok(())
}

/// Unregister volume information, add the volume to `node_id` list mapping
pub async fn unregister_volume(
    etcd_client: &EtcdDelegate,
    node_id: &str,
    volume_info: &str,
) -> anyhow::Result<()> {
    let lock_key = etcd_client
        .lock(ETCD_VOLUME_INFO_LOCK.as_bytes(), 10)
        .await
        .with_context(|| "lock fail while register volume")?;

    let volume_info_key = format!("{ETCD_VOLUME_INFO_PREFIX}{volume_info}");
    let volume_node_list: Option<Vec<u8>> = etcd_client
        .get_at_most_one_value(volume_info_key.as_str())
        .await
        .with_context(|| format!("get {volume_info_key} from etcd fail"))?;

    if let Some(volume_node_list)= volume_node_list{
        let mut node_set: HashSet<String> = bincode::deserialize(volume_node_list.as_slice())
            .unwrap_or_else(|e| {
                panic!("fail to deserialize node list for volume {volume_info:?}, error: {e}");
            });
        node_set.remove(node_id);
        let volume_node_list_bin = bincode::serialize(&node_set).unwrap_or_else(|e| {
            panic!("fail to serialize node list for volume {volume_info:?}, error: {e}")
        });
    
        etcd_client
            .write_or_update_kv(volume_info_key.as_str(), &volume_node_list_bin)
            .await
            .with_context(|| {
                format!(
                    "Update Volume to Node Id mapping failed, volume:{volume_info}, node id: {node_id}"
                )
            })?;
    
        etcd_client
            .unlock(lock_key)
            .await
            .with_context(|| "unlock fail while register volume")?;
    }
    
    Ok(())
}

/// Get node list related to a volume, execluding the input `node_ide` as its the local node id.
/// This function is used to sync metadata, the inode information.
pub async fn get_volume_nodes(
    etcd_client: Arc<EtcdDelegate>,
    node_id: &str,
    volume_info: &str,
) -> anyhow::Result<HashSet<String>> {
    let volume_info_key = format!("{ETCD_VOLUME_INFO_PREFIX}{volume_info}");
    let volume_node_list: Option<Vec<u8>> = etcd_client
        .get_at_most_one_value(volume_info_key.as_str())
        .await
        .with_context(|| format!("get {volume_info_key} from etcd fail"))?;

    let new_volume_node_list = if let Some(node_list) = volume_node_list {
        let mut node_set: HashSet<String> = bincode::deserialize(node_list.as_slice())
            .unwrap_or_else(|e| {
                panic!("fail to deserialize node list for volume {volume_info:?}, error: {e}");
            });

        debug!("node set when get volume related node, {:?}", node_set);

        if node_set.contains(node_id) {
            node_set.remove(node_id);
        }

        node_set
    } else {
        debug!("node set is empty");
        HashSet::new()
    };

    Ok(new_volume_node_list)
}

/// Modify node list of a file
async fn modify_file_node_list<F: Fn(Option<Vec<u8>>) -> HashSet<String>>(
    etcd_client: &EtcdDelegate,
    file_name: &[u8],
    fun: F,
) -> anyhow::Result<()>
where
    F: Send,
{
    let mut file_lock_key = ETCD_FILE_NODE_LIST_LOCK_PREFIX.as_bytes().to_vec();
    file_lock_key.extend_from_slice(file_name);

    let lock_key = etcd_client
        .lock(file_lock_key.as_slice(), 10)
        .await
        .with_context(|| "lock fail update file node list")?;

    let mut node_list_key = ETCD_FILE_NODE_LIST_PREFIX.as_bytes().to_vec();
    node_list_key.extend_from_slice(file_name);
    let node_list_key_clone = node_list_key.clone();

    let node_list: Option<Vec<u8>> = etcd_client
        .get_at_most_one_value(node_list_key)
        .await
        .with_context(|| format!("get {ETCD_NODE_ID_COUNTER_KEY} from etcd fail"))?;

    let new_node_list = fun(node_list);

    let node_list_bin = bincode::serialize(&new_node_list).unwrap_or_else(|e| {
        panic!("fail to serialize node list for file {file_name:?}, error: {e}")
    });

    let node_list_key_clone_2 = node_list_key_clone.clone();
    etcd_client
        .write_or_update_kv(node_list_key_clone, &node_list_bin)
        .await
        .with_context(|| {
            format!("update {node_list_key_clone_2:?} to value {node_list_bin:?} failed")
        })?;

    etcd_client
        .unlock(lock_key)
        .await
        .with_context(|| "unlock fail while update file node list")?;

    Ok(())
}

/// Add a node to node list of a file
pub async fn add_node_to_file_list(
    etcd_client: &EtcdDelegate,
    node_id: &str,
    file_name: &[u8],
) -> anyhow::Result<()> {
    let add_node_fun = |node_list: Option<Vec<u8>>| -> HashSet<String> {
        node_list.map_or_else(
            || {
                let mut node_set = HashSet::<String>::new();
                node_set.insert(node_id.to_owned());
                node_set
            },
            |list| {
                let mut node_set: HashSet<String> = bincode::deserialize(list.as_slice())
                    .unwrap_or_else(|e| {
                        panic!("fail to deserialize node list for file {file_name:?}, error: {e}");
                    });

                if !node_set.contains(node_id) {
                    node_set.insert(node_id.to_owned());
                }

                node_set
            },
        )
    };

    modify_file_node_list(etcd_client, file_name, add_node_fun).await
}

/// Remove a node to node list of a file
pub async fn remove_node_from_file_list(
    etcd_client: &EtcdDelegate,
    node_id: &str,
    file_name: &[u8],
) -> anyhow::Result<()> {
    let remove_node_fun = |node_list: Option<Vec<u8>>| -> HashSet<String> {
        match node_list {
            Some(list) => {
                let mut node_set: HashSet<String> = bincode::deserialize(list.as_slice())
                    .unwrap_or_else(|e| {
                        panic!("fail to deserialize node list for file {file_name:?}, error: {e}");
                    });

                if node_set.contains(node_id) {
                    node_set.remove(node_id);
                }

                node_set
            }
            None => HashSet::<String>::new(),
        }
    };

    modify_file_node_list(etcd_client, file_name, remove_node_fun).await
}

/// Get the ETCD lock for inode number
pub async fn lock_inode_number(etcd_client: Arc<EtcdDelegate>) -> anyhow::Result<Vec<u8>> {
    let lock_key = etcd_client
        .lock(ETCD_INODE_NUMBER_LOCK.as_bytes(), 10)
        .await
        .with_context(|| "lock fail update file node list")?;
    Ok(lock_key)
}

/// Release the ETCD lock for inode number
pub async fn unlock_inode_number(
    etcd_client: Arc<EtcdDelegate>,
    lock_key: Vec<u8>,
) -> anyhow::Result<()> {
    etcd_client
        .unlock(lock_key)
        .await
        .with_context(|| "unlock fail while update file node list")?;
    Ok(())
}

/// increase the inode number range begin in global cluster
pub async fn fetch_add_inode_next_range(
    etcd_client: Arc<EtcdDelegate>,
    range: u64,
) -> anyhow::Result<(INum, INum)> {
    // Use cas to replace the lock
    // Lock before rewrite
    let lockkey = lock_inode_number(Arc::clone(&etcd_client)).await?;
    let inode_range_begin: Option<Vec<u8>> = etcd_client
        .get_at_most_one_value(ETCD_INODE_NEXT_RANGE.as_bytes())
        .await
        .with_context(|| {
            format!("get ETCD_INODE_NEXT_RANGE {ETCD_INODE_NEXT_RANGE} from etcd fail")
        })?;

    // Read inode range begin from etcd
    let inode_range_begin = match inode_range_begin {
        Some(number) => {
            let number: INum = bincode::deserialize(number.as_slice()).unwrap_or_else(|e| {
                panic!("fail to deserialize inode number from etcd, error: {e}");
            });
            number
        }
        //1 is the root inode
        None => 2,
    };
    // Add up and store data back to etcd
    let next = inode_range_begin.add(range);
    etcd_client
        .write_or_update_kv(
            ETCD_INODE_NEXT_RANGE,
            &bincode::serialize(&next).unwrap_or_else(|e| panic!("serialize inum failed, err:{e}")),
        )
        .await?;
    unlock_inode_number(etcd_client, lockkey).await?;
    debug!("node alloc inum range ({},{})", inode_range_begin, next);
    Ok((inode_range_begin, next))
}

/// Mark a path with ino in etcd
/// Only one can mark path successfully
pub async fn mark_fullpath_with_ino_in_etcd(
    etcd_client: &Arc<EtcdDelegate>,
    volume: &str,
    fullpath: &str,
    ino: u64,
) -> anyhow::Result<INum> {
    /// timeout for mark if unmark fails.
    const TIMEOUT: Duration = Duration::from_secs(15);
    let key = format!("{ETCD_INODE_MARK_PREFIX}{volume}{fullpath}");
    match etcd_client
        .write_new_kv_no_panic(key.as_str(), &ino, Some(TIMEOUT))
        .await
        .with_context(|| format!("mark_fullpath_with_ino_in_etcd {fullpath} {ino}"))?
    {
        None => Ok(ino),
        Some(oldino) => Ok(oldino),
    }
}

/// Unmark a path with ino in etcd
/// Remember to unmark after binded inum to node
pub async fn unmark_fullpath_with_ino_in_etcd(
    etcd_client: Arc<EtcdDelegate>,
    volume: &str,
    fullpath: String,
) {
    let key = format!("{ETCD_INODE_MARK_PREFIX}{volume}{fullpath}");
    match etcd_client
        .delete_exact_one_value::<INum>(key.as_str())
        .await
    {
        Ok(_) => {}
        Err(e) => {
            // It's ok to fail, we have a lease for expire
            debug!("unmark_fullpath_with_ino_in_etcd failed with error {}", e);
        }
    }
}
