use std::net::IpAddr;

/// Volume type
#[derive(Clone, Copy, Debug)]
pub enum VolumeType {
    /// Do nothing S3 volume
    None,
    /// S3 volume
    S3,
    /// Local volume
    Local,
}

/// Async fuse args type
#[derive(Debug)]
pub struct AsyncFuseArgs {
    /// Node id
    pub node_id: String,
    /// Node ip
    pub ip_address: IpAddr,
    /// Server port
    pub server_port: String,
    /// Volume type
    pub volume_type: VolumeType,
    /// Mount dir
    pub mount_dir: String,
    /// Cache capacity
    pub cache_capacity: usize,
    /// Volume info
    pub volume_info: String,
}