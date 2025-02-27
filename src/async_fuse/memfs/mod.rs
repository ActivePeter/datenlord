//! The implementation of user space file system
mod cache;
mod dir;
/// distributed communication module
pub mod dist;
mod fs_util;
mod id_alloc_used;
/// The KV engine module
#[macro_use]
pub mod kv_engine;
/// fs metadata module
mod metadata;
mod node;
/// Persist module
mod persist;
/// fs metadata with S3 backend module
mod s3_metadata;
mod s3_node;
/// S3 backend wrapper module
pub mod s3_wrapper;
/// Serializable types module
pub mod serial;

use std::os::unix::prelude::RawFd;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use clippy_utilities::Cast;
use log::{debug, warn};
use nix::errno::Errno;
use nix::sys::stat::SFlag;

use crate::async_fuse::fuse::file_system;
use crate::async_fuse::fuse::file_system::FileSystem;
use crate::async_fuse::fuse::file_system::FsAsyncTaskController;
use crate::async_fuse::fuse::fuse_reply::AsIoVec;
use crate::async_fuse::fuse::fuse_reply::{
    ReplyAttr, ReplyBMap, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyLock, ReplyOpen, ReplyStatFs, ReplyWrite, ReplyXAttr,
};
use crate::async_fuse::fuse::fuse_request::Request;
use crate::async_fuse::fuse::protocol::{INum, FUSE_ROOT_ID};
use crate::common::error::{Context, DatenLordResult};
use crate::common::etcd_delegate::EtcdDelegate;
use cache::IoMemBlock;
use dist::server::CacheServer;
pub use metadata::DefaultMetaData;
pub use metadata::MetaData;
pub use s3_metadata::S3MetaData;
use serde::{Deserialize, Serialize};

use self::kv_engine::KVEngineType;

use super::fuse::file_system::FsController;

/// In-memory file system
#[derive(Debug)]
pub struct MemFs<M: MetaData + Send + Sync + 'static> {
    /// Fs metadata
    metadata: Arc<M>,
    #[allow(dead_code)]
    /// Cache server
    server: Option<CacheServer>,
}

/// Set attribute parameters
#[derive(Debug)]
pub struct SetAttrParam {
    /// FUSE set attribute bit mask
    pub valid: u32,
    /// File handler
    pub fh: Option<u64>,
    /// File mode
    pub mode: Option<u32>,
    /// User ID
    pub u_id: Option<u32>,
    /// Group ID
    pub g_id: Option<u32>,
    /// File size
    pub size: Option<u64>,
    /// Lock owner
    #[cfg(feature = "abi-7-9")]
    pub lock_owner: Option<u64>,
    /// Access time
    pub a_time: Option<SystemTime>,
    /// Content modified time
    pub m_time: Option<SystemTime>,
    /// Meta-data changed time seconds
    #[cfg(feature = "abi-7-23")]
    pub c_time: Option<SystemTime>,
    /// Creation time, macOS only
    #[cfg(target_os = "macos")]
    pub crtime: Option<SystemTime>,
    /// macOS only
    #[cfg(target_os = "macos")]
    pub chgtime: Option<SystemTime>,
    /// Backup time, macOS only
    #[cfg(target_os = "macos")]
    pub bkuptime: Option<SystemTime>,
    /// See chflags(2)
    #[cfg(target_os = "macos")]
    pub flags: Option<u32>,
}

/// Rename parameters
#[derive(Serialize, Deserialize, Debug)]
pub struct RenameParam {
    /// Old parent directory i-number
    pub old_parent: INum,
    /// Old name
    pub old_name: String,
    /// New parent directory i-number
    pub new_parent: INum,
    /// New name
    pub new_name: String,
    /// Rename flags
    pub flags: u32,
}

/// POSIX file lock parameters
#[derive(Debug)]
pub struct FileLockParam {
    /// File hander
    pub fh: u64,
    /// Lock owner
    pub lock_owner: u64,
    /// Start offset
    pub start: u64,
    /// End offset
    pub end: u64,
    /// Lock type
    pub typ: u32,
    /// The process ID of the lock
    pub pid: u32,
}

impl<M: MetaData + Send + Sync + 'static> MemFs<M> {
    /// Create `FileSystem`
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        mount_point: &str,
        capacity: usize,
        ip: &str,
        port: &str,
        etcd_client: EtcdDelegate,
        kv_engine: Arc<KVEngineType>,
        node_id: &str,
        volume_info: &str,
    ) -> anyhow::Result<(Self, FsController)> {
        let (sender, receiver) = file_system::new_fs_async_result_chan();
        let (metadata, server, async_task_join_handles) = M::new(
            mount_point,
            capacity,
            ip,
            port,
            etcd_client,
            kv_engine,
            node_id,
            volume_info,
            sender,
        )
        .await;
        Ok((
            Self { metadata, server },
            FsController::new(receiver, async_task_join_handles),
        ))
    }

    /// Read content check
    fn read_helper(content: Vec<IoMemBlock>, size: usize) -> DatenLordResult<Vec<IoMemBlock>> {
        if content.iter().filter(|c| !c.can_convert()).count() > 0 {
            return super::util::build_error_result_from_errno(
                Errno::EIO,
                "The content is out of scope".to_owned(),
            );
        }
        let content_total_len: usize = content.iter().map(IoMemBlock::len).sum();
        debug!("read {} data, expected size {}", content_total_len, size);
        Ok(content)
    }
}

#[async_trait]
impl<M: MetaData + Send + Sync + 'static> FileSystem for MemFs<M> {
    // Implemented FUSE operations

    /// Initialize filesystem.
    /// Called before any other filesystem method.
    async fn init(&self, req: &Request<'_>) -> nix::Result<()> {
        let cache = self.metadata.cache().read().await;
        debug!("init(req={:?}), cache size={}", req, cache.len(),);
        Ok(())
    }

    /// Clean up filesystem.
    /// Called on filesystem exit.
    async fn destroy(&self, req: &Request<'_>) {
        let cache = self.metadata.cache().read().await;
        debug!("destroy(req={:?}), cache size={}", req, cache.len(),);
    }

    /// Look up a directory entry by name and get its attributes.
    async fn lookup(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        reply: ReplyEntry,
    ) -> nix::Result<usize> {
        debug!("lookup(parent={}, name={:?}, req={:?})", parent, name, req,);
        let lookup_res = self.metadata.lookup_helper(parent, name).await;
        match lookup_res {
            Ok((ttl, fuse_attr, generation)) => {
                debug!(
                    "lookup() successfully found the node name={:?} \
                        under parent ino={}, the attr={:?}",
                    name, parent, &fuse_attr,
                );
                reply.entry(ttl, fuse_attr, generation).await
            }
            Err(e) => {
                debug!(
                    "lookup() failed to find the node name={:?} under parent ino={}, \
                        the error is: {}",
                    name, parent, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Get file attributes.
    async fn getattr(&self, req: &Request<'_>, reply: ReplyAttr) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("getattr(ino={}, req={:?})", ino, req);
        match self.metadata.getattr(ino).await {
            Ok((ttl, fuse_attr)) => {
                debug!(
                    "getattr() successfully got the attr={:?} of ino={}",
                    fuse_attr, ino,
                );
                reply.attr(ttl, fuse_attr).await
            }
            Err(err) => {
                // In the previous version ,this panic will never happen.
                // Later, we will
                panic!("getattr() failed to get the attr of ino={ino}, the error is: {err}",);
            }
        }
    }

    /// Open a file.
    /// Open flags (with the exception of `O_CREAT`, `O_EXCL`, `O_NOCTTY` and `O_TRUNC`) are
    /// available in flags. Filesystem may store an arbitrary file handle (pointer, index,
    /// etc) in fh, and use self in other all other file operations (read, write, flush,
    /// release, fsync). Filesystem may also implement stateless file I/O and not store
    /// anything in fh. There are also some flags (`direct_io`, `keep_cache`) which the
    /// filesystem may set, to change the way the file is opened. See `fuse_file_info`
    /// structure in `fuse_common.h` for more details.
    async fn open(&self, req: &Request<'_>, flags: u32, reply: ReplyOpen) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("open(ino={}, flags={}, req={:?})", ino, flags, req);

        match self.metadata.open(ino, flags).await {
            Ok(new_fd) => {
                debug!(
                    "open() successfully duplicated the file handler of ino={} , fd={}, flags={:?}",
                    ino, new_fd, flags,
                );
                reply.opened(new_fd, flags).await
            }
            Err(e) => {
                debug!("open() failed, the error is: {}", e);
                reply.error(e).await
            }
        }
    }

    /// Forget about an inode.
    /// The nlookup parameter indicates the number of lookups previously performed on
    /// self inode. If the filesystem implements inode lifetimes, it is recommended that
    /// inodes acquire a single reference on each lookup, and lose nlookup references on
    /// each forget. The filesystem may ignore forget calls, if the inodes don't need to
    /// have a limited lifetime. On unmount it is not guaranteed, that all referenced
    /// inodes will receive a forget message.
    async fn forget(&self, req: &Request<'_>, nlookup: u64) {
        let ino = req.nodeid();
        debug!("forget(ino={}, nlookup={}, req={:?})", ino, nlookup, req,);
        self.metadata.forget(ino, nlookup).await;
    }

    /// Set file attributes.
    async fn setattr(
        &self,
        req: &Request<'_>,
        param: SetAttrParam,
        reply: ReplyAttr,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        let valid = param.valid;
        let fh = param.fh;
        let mode = param.mode;
        let u_id = param.u_id;
        let g_id = param.g_id;
        let size = param.size;
        let a_time = param.a_time;
        let m_time = param.m_time;
        #[cfg(feature = "abi-7-9")]
        let _lock_owner = param.lock_owner;
        #[cfg(feature = "abi-7-23")]
        let _c_time = param.c_time;
        debug!(
            "setattr(ino={}, valid={:?}, mode={:?}, uid={:?}, gid={:?}, size={:?}, \
                atime={:?}, mtime={:?}, fh={:?}, req={:?})",
            ino,
            valid,
            mode.map(|bits| format!("{bits:#o}")),
            u_id,
            g_id,
            size,
            a_time,
            m_time,
            fh,
            req,
        );
        if 0 == valid {
            warn!("setattr() enountered valid=0, the req={:?}", req);
        };
        let set_res = self.metadata.setattr_helper(ino, param).await;
        match set_res {
            Ok((ttl, fuse_attr)) => reply.attr(ttl, fuse_attr).await,
            Err(e) => reply.error(e).await,
        }
    }

    /// Create file node.
    /// Create a regular file, character device, block device, fifo or socket node.
    async fn mknod(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        mode: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) -> nix::Result<usize> {
        debug!(
            "mknod(parent={}, name={:?}, mode={}, rdev={}, req={:?})",
            parent, name, mode, rdev, req,
        );
        let mknod_res = self
            .metadata
            .create_node_helper(parent, name, mode, SFlag::S_IFREG, None)
            .await
            .add_context(format!(
                "mknod() failed to create an i-node name={name:?} and mode={mode:?} under parent ino={parent},",
            ));
        match mknod_res {
            Ok((ttl, fuse_attr, generation)) => reply.entry(ttl, fuse_attr, generation).await,
            Err(e) => {
                debug!(
                    "mknod() failed to create an i-node name={:?} and mode={:?} under parent ino={}, \
                        the error is: {}",
                    name,
                    mode,
                    parent,
                    e,
                );
                reply.error(e).await
            }
        }
    }

    /// Create a directory.
    async fn mkdir(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        mode: u32,
        reply: ReplyEntry,
    ) -> nix::Result<usize> {
        debug!(
            "mkdir(parent={}, name={:?}, mode={}, req={:?})",
            parent, name, mode, req,
        );
        let mkdir_res = self
            .metadata
            .create_node_helper(parent, name, mode, SFlag::S_IFDIR, None)
            .await
            .add_context(format!(
                "mkdir() failed to create a directory name={name:?} and mode={mode:?} under parent ino={parent}",
            ));
        match mkdir_res {
            Ok((ttl, fuse_attr, generation)) => reply.entry(ttl, fuse_attr, generation).await,
            Err(e) => {
                debug!(
                    "mkdir() failed to create a directory name={:?} and mode={:?} under parent ino={}, \
                        the error is: {}",
                    name,
                    mode,
                    parent,
                    e,
                );
                reply.error(e).await
            }
        }
    }

    /// Remove a file.
    async fn unlink(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        debug!("unlink(parent={}, name={:?}, req={:?}", parent, name, req,);
        match self.metadata.unlink(parent, name).await {
            Ok(()) => reply.ok().await,
            Err(e) => {
                debug!(
                    "unlink() failed to remove file name={:?} under parent ino={}, \
                        the error is: {}",
                    name, parent, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Remove a directory.
    async fn rmdir(
        &self,
        req: &Request<'_>,
        parent: INum,
        dir_name: &str,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        debug!(
            "rmdir(parent={}, name={:?}, req={:?})",
            parent, dir_name, req,
        );
        let rmdir_res = self
            .metadata
            .remove_node_helper(parent, dir_name, SFlag::S_IFDIR)
            .await
            .add_context(format!(
                "rmdir() failed to remove sub-directory name={dir_name:?} under parent ino={parent}",
            ));
        match rmdir_res {
            Ok(()) => reply.ok().await,
            Err(e) => {
                debug!(
                    "rmdir() failed to remove sub-directory name={:?} under parent ino={}, \
                            the error is: {}",
                    dir_name, parent, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Rename a file
    ///
    /// If the target exists it should be atomically replaced. If
    /// the target's inode's lookup count is non-zero, the file
    /// system is expected to postpone any removal of the inode
    /// until the lookup count reaches zero (see description of the
    /// forget function).
    ///
    /// *flags* may be `RENAME_EXCHANGE` or `RENAME_NOREPLACE`. If
    /// `RENAME_NOREPLACE` is specified, the filesystem must not
    /// overwrite *newname* if it exists and return an error
    /// instead. If `RENAME_EXCHANGE` is specified, the filesystem
    /// must atomically exchange the two files, i.e. both must
    /// exist and neither may be deleted.
    async fn rename(
        &self,
        _: &Request<'_>,
        param: RenameParam,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let exchange = param.flags == 2; // RENAME_EXCHANGE
        let rename_res = if exchange {
            self.metadata.rename_exchange_helper(param).await
        } else {
            // Rename replace
            self.metadata.rename_may_replace_helper(param).await
        };

        match rename_res {
            Ok(()) => reply.ok().await,
            Err(e) => {
                debug!("rename() failed, the error is: {}", e);
                reply.error(e).await
            }
        }
    }

    /// Read data.
    /// Read should send exactly the number of bytes requested except on EOF or error,
    /// otherwise the rest of the data will be substituted with zeroes. An exception to
    /// self is when the file has been opened in `direct_io` mode, in which case the
    /// return value of the read system call will reflect the return value of self
    /// operation. fh will contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value.
    async fn read(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "read(ino={}, fh={}, offset={}, size={}, req={:?})",
            ino, fh, offset, size, req,
        );
        debug_assert!(!offset.is_negative(), "offset={offset} cannot be negative");
        let file_data = match self.metadata.read_helper(ino, fh, offset, size).await {
            Ok(file_data) => file_data,
            Err(e) => {
                return reply.error(e).await;
            }
        };
        debug!("file_data is {:?}", file_data);
        match Self::read_helper(file_data, size.cast()) {
            Ok(content) => {
                debug!(
                    "read() successfully read {} bytes from the file of ino={}",
                    content.iter().map(AsIoVec::len).sum::<usize>(),
                    ino,
                );
                reply.data(content).await
            }
            Err(e) => {
                debug!(
                    "read() failed to read from the file of ino={}, the error is: {}",
                    ino, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Write data.
    /// Write should return exactly the number of bytes requested except on error. An
    /// exception to self is when the file has been opened in `direct_io` mode, in
    /// which case the return value of the write system call will reflect the return
    /// value of self operation. fh will contain the value set by the open method, or
    /// will be undefined if the open method did not set any value.
    async fn write(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        flags: u32,
        reply: ReplyWrite,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "write(ino={}, fh={}, offset={}, data-size={}, flags={})",
            // "write(ino={}, fh={}, offset={}, data-size={}, req={:?})",
            ino,
            fh,
            offset,
            data.len(),
            flags,
            // req.request,
        );
        let data_len = data.len();
        let write_result = self
            .metadata
            .write_helper(ino, fh, offset, data, flags)
            .await;
        match write_result {
            Ok(written_size) => {
                debug!(
                    "write() successfully wrote {} byte data to \
                        the file of ino={} at offset={}",
                    data_len, ino, offset,
                );
                reply.written(written_size.cast()).await
            }
            Err(e) => {
                debug!(
                    "write() failed to write to the file of ino={} at offset={}, \
                        the error is: {}",
                    ino, offset, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Flush method.
    /// This is called on each close() of the opened file. Since file descriptors can
    /// be duplicated (dup, dup2, fork), for one open call there may be many flush
    /// calls. Filesystems should not assume that flush will always be called after some
    /// writes, or that if will be called at all. fh will contain the value set by the
    /// open method, or will be undefined if the open method did not set any value.
    /// NOTE: the name of the method is misleading, since (unlike fsync) the filesystem
    /// is not forced to flush pending writes. One reason to flush data, is if the
    /// filesystem wants to return write errors. If the filesystem supports file locking
    /// operations (setlk, getlk) it should remove all locks belonging to `lock_owner`.
    async fn flush(
        &self,
        req: &Request<'_>,
        fh: u64,
        lock_owner: u64,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "flush(ino={}, fh={}, lock_owner={}, req={:?})",
            ino, fh, lock_owner, req,
        );
        // This is called from every close on an open file, so call the
        // close on the underlying filesystem.	But since flush may be
        // called multiple times for an open file, self must not really
        // close the file. This is important if used on a network
        // filesystem like NFS which flush the data/metadata on close()
        self.metadata.flush(ino, fh).await;
        reply.ok().await
    }

    /// Release an open file.
    /// Release is called when there are no more references to an open file: all file
    /// descriptors are closed and all memory mappings are unmapped. For every open
    /// call there will be exactly one release call. The filesystem may reply with an
    /// error, but error values are not returned to close() or munmap() which triggered
    /// the release. fh will contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value. flags will contain the same flags as for
    /// open.
    async fn release(
        &self,
        req: &Request<'_>,
        fh: u64,
        flags: u32, // same as the open flags
        lock_owner: u64,
        flush: bool,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "release(ino={}, fh={}, flags={}, lock_owner={}, flush={}, req={:?})",
            ino, fh, flags, lock_owner, flush, req,
        );
        self.metadata
            .release(ino, fh, flags, lock_owner, flush)
            .await;
        reply.ok().await
    }

    /// Synchronize file contents.
    /// If the datasync parameter is non-zero, then only the user data should be flushed,
    /// not the meta data.
    async fn fsync(
        &self,
        req: &Request<'_>,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();

        debug!(
            "fsync(ino={}, fh={}, datasync={}, req={:?})",
            ino, fh, datasync, req,
        );
        match self.metadata.fsync_helper(ino, fh, datasync).await {
            Ok(()) => reply.ok().await,
            Err(e) => {
                debug!("fsync() failed, the error is: {}", e);
                reply.error(e).await
            }
        }
    }

    /// Open a directory.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh, and
    /// use self in other all other directory stream operations (readdir, releasedir,
    /// fsyncdir). Filesystem may also implement stateless directory I/O and not store
    /// anything in fh, though that makes it impossible to implement standard conforming
    /// directory stream operations in case the contents of the directory can change
    /// between opendir and releasedir.
    async fn opendir(&self, req: &Request<'_>, flags: u32, reply: ReplyOpen) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("opendir(ino={}, flags={}, req={:?})", ino, flags, req,);
        let o_flags = fs_util::parse_oflag(flags);
        match self.metadata.opendir(ino, flags).await {
            Ok(new_fd) => {
                debug!(
                    "opendir() successfully duplicated the file handler of \
                        ino={}  with flags={:?}, the new fd={}",
                    ino, o_flags, new_fd,
                );
                reply.opened(new_fd, flags).await
            }
            Err(e) => {
                debug!(
                    "opendir() failed to duplicate the file handler of ino={} with flags={:?}, \
                        the error is: {}",
                    ino, o_flags, e
                );
                reply.error(e).await
            }
        }
    }

    /// Read directory.
    /// Send a buffer filled using buffer.fill(), with size not exceeding the
    /// requested size. Send an empty buffer on end of stream. fh will contain the
    /// value set by the opendir method, or will be undefined if the opendir method
    /// didn't set any value.
    async fn readdir(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        reply: ReplyDirectory,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "readdir(ino={}, fh={}, offset={}, req={:?})",
            ino, fh, offset, req,
        );

        self.metadata.readdir(ino, fh, offset, reply).await
    }

    /// Release an open directory.
    /// For every opendir call there will be exactly one releasedir call. fh will
    /// contain the value set by the opendir method, or will be undefined if the
    /// opendir method didn't set any value.
    async fn releasedir(
        &self,
        req: &Request<'_>,
        fh: u64,
        flags: u32,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "releasedir(ino={}, fh={}, flags={}, req={:?})",
            ino, fh, flags, req,
        );
        // TODO: handle flags
        self.metadata.releasedir(ino, fh).await;
        reply.ok().await
    }

    /// Synchronize directory contents.
    /// If the datasync parameter is set, then only the directory contents should
    /// be flushed, not the meta data. fh will contain the value set by the opendir
    /// method, or will be undefined if the opendir method didn't set any value.
    async fn fsyncdir(
        &self,
        req: &Request<'_>,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "fsyncdir(ino={}, fh={}, datasync={}, req={:?})",
            ino, fh, datasync, req,
        );
        // Self::fsync_helper(ino, fh, datasync, reply).await
        match self.metadata.fsync_helper(ino, fh, datasync).await {
            Ok(()) => reply.ok().await,
            Err(e) => {
                debug!("fsyncdir() failed, the error is: {}", e);
                reply.error(e).await
            }
        }
    }

    /// Get file system statistics.
    /// The `f_favail`, `f_fsid` and `f_flag` fields are ignored
    async fn statfs(&self, req: &Request<'_>, reply: ReplyStatFs) -> nix::Result<usize> {
        let ino = if req.nodeid() == 0 {
            FUSE_ROOT_ID
        } else {
            req.nodeid()
        };
        debug!("statfs(ino={}, req={:?})", ino, req);
        match self.metadata.statfs(ino).await {
            Ok(statvfs) => {
                debug!(
                    "statfs() successfully read the statvfs of ino={} the statvfs={:?}",
                    ino, statvfs,
                );
                reply.statfs(statvfs).await
            }
            Err(e) => {
                debug!(
                    "statfs() failed to read the statvfs of ino={}  the error is: {}",
                    ino, e
                );
                reply.error(e).await
            }
        }
    }

    /// Read symbolic link.
    async fn readlink(&self, req: &Request<'_>, reply: ReplyData) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("readlink(ino={}, req={:?})", ino, req,);
        reply.data(self.metadata.readlink(ino).await).await
    }

    /// Create a symbolic link.
    async fn symlink(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        target_path: &Path,
        reply: ReplyEntry,
    ) -> nix::Result<usize> {
        debug!(
            "symlink(parent={}, name={:?}, target_path={:?}, req={:?})",
            parent, name, target_path, req
        );
        let symlink_res = self.metadata.create_node_helper(
            parent,
            name,
            0o777, // Symbolic links have no permissions
            SFlag::S_IFLNK,
            Some(target_path),
        )
        .await
        .add_context(format!(
            "symlink() failed to create a symlink name={name:?} to target path={target_path:?} under parent ino={parent}",
        ));
        match symlink_res {
            Ok((ttl, fuse_attr, generation)) => reply.entry(ttl, fuse_attr, generation).await,
            Err(e) => {
                debug!(
                    "symlink() failed to create a symlink name={:?} to target path={:?} under parent ino={}, \
                        the error is: {}",
                    name,
                    target_path,
                    parent,
                    e,
                );
                reply.error(e).await
            }
        }
    }

    // Un-implemented FUSE operations

    /// Interrupt another FUSE request
    async fn interrupt(&self, req: &Request<'_>, unique: u64) {
        let cache = self.metadata.cache().read().await;
        debug!("interrupt(req={:?}), cache size={}", req, cache.len(),);
        // TODO: handle FUSE_INTERRUPT
        warn!(
            "FUSE INTERRUPT recieved, request w/ unique={} interrupted",
            unique
        );
    }

    /// Create a hard link.
    async fn link(
        &self,
        _req: &Request<'_>,
        _newparent: u64,
        _newname: &str,
        reply: ReplyEntry,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Set an extended attribute.
    async fn setxattr(
        &self,
        _req: &Request<'_>,
        _name: &str,
        _value: &[u8],
        _flags: u32,
        _position: u32,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Get an extended attribute.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    async fn getxattr(
        &self,
        _req: &Request<'_>,
        _name: &str,
        _size: u32,
        reply: ReplyXAttr,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// List extended attribute names.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    async fn listxattr(
        &self,
        _req: &Request<'_>,
        _size: u32,
        reply: ReplyXAttr,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Remove an extended attribute.
    async fn removexattr(
        &self,
        _req: &Request<'_>,
        _name: &str,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Check file access permissions.
    /// This will be called for the `access()` system call. If the `default_permissions`
    /// mount option is given, self method is not called. This method is not called
    /// under Linux kernel versions 2.4.x
    async fn access(
        &self,
        _req: &Request<'_>,
        _mask: u32,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Create and open a file.
    /// If the file does not exist, first create it with the specified mode, and then
    /// open it. Open flags (with the exception of `O_NOCTTY`) are available in flags.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh,
    /// and use self in other all other file operations (read, write, flush, release,
    /// fsync). There are also some flags (`direct_io`, `keep_cache`) which the
    /// filesystem may set, to change the way the file is opened. See `fuse_file_info`
    /// structure in `fuse_common.h` for more details. If self method is not
    /// implemented or under Linux kernel versions earlier than 2.6.15, the mknod()
    /// and open() methods will be called instead.
    async fn create(
        &self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &str,
        _mode: u32,
        _flags: u32,
        reply: ReplyCreate,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Test for a POSIX file lock.
    async fn getlk(
        &self,
        _req: &Request<'_>,
        _lk_param: FileLockParam,
        reply: ReplyLock,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Acquire, modify or release a POSIX file lock.
    /// For POSIX threads (NPTL) there's a 1-1 relation between pid and owner, but
    /// otherwise self is not always the case.  For checking lock ownership,
    /// `fi->owner` must be used. The `l_pid` field in `struct flock` should only be
    /// used to fill in self field in `getlk()`. Note: if the locking methods are not
    /// implemented, the kernel will still allow file locking to work locally.
    /// Hence these are only interesting for network filesystems and similar.
    async fn setlk(
        &self,
        _req: &Request<'_>,
        _lk_param: FileLockParam,
        _sleep: bool,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Map block index within file to block index within device.
    /// Note: This makes sense only for block device backed filesystems mounted
    /// with the `blkdev` option
    async fn bmap(
        &self,
        _req: &Request<'_>,
        _blocksize: u32,
        _idx: u64,
        reply: ReplyBMap,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Set fuse fd into `FileSystem`
    async fn set_fuse_fd(&self, fuse_fd: RawFd) {
        self.metadata.set_fuse_fd(fuse_fd).await;
    }
}

#[async_trait]
impl<M: MetaData + Send + Sync + 'static> FsAsyncTaskController for MemFs<M> {
    /// Stop all async tasks
    fn stop_all_async_tasks(&self) {
        self.metadata.stop_all_async_tasks();
    }
}

#[cfg(test)]
mod test {

    use nix::sys::statvfs;
    use std::fs::File;
    #[test]
    fn test_statfs() -> anyhow::Result<()> {
        let file = File::open(".")?;
        let statvfs = statvfs::fstatvfs(&file)?;
        println!(
            "blocks={}, bfree={}, bavail={}, files={}, \
                ffree={}, bsize={}, namelen={}, frsize={}",
            statvfs.blocks(),
            statvfs.blocks_free(),
            statvfs.blocks_available(),
            statvfs.files(),
            statvfs.files_free(),
            statvfs.block_size(),
            statvfs.name_max(),
            statvfs.fragment_size(),
        );
        Ok(())
    }
}
