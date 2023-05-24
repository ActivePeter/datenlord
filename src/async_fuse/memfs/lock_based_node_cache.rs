struct LockBasedNodeCache {
    /// Opened files
    locked_nodes_cache: RwLock<HashMap<INum, S3Node<S>>>,
}
