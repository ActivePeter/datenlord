use std::sync::Arc;
use clippy_utilities::OverflowArithmetic;
use tokio::sync::Mutex;
use crate::async_fuse::fuse::protocol::INum;
use crate::async_fuse::memfs::{dist, S3MetaData};
use crate::async_fuse::memfs::dist::etcd;
use crate::async_fuse::memfs::s3_wrapper::S3BackEnd;
use crate::common::etcd_delegate::EtcdDelegate;

#[derive(Debug)]
pub(crate) struct InodeState {
    /// Use tokio mutex to avoid block when waiting for etcd.
    range_begin_end:Mutex<(INum,INum)>,
    recycle_unused:crossbeam_queue::SegQueue<INum>,
}

impl InodeState {
    pub fn new()->Self{
        Self{
            range_begin_end:Mutex::new((0,0)),
            recycle_unused: Default::default(),
        }
    }
}

impl InodeState {
    /// just get a unique inum
    async fn alloc_inum(&self, etcd_client:&Arc<EtcdDelegate>) -> INum {
        const INODE_RANGE:u64=10000;
        if let Some(inum)= self.recycle_unused.pop(){
            return inum;
        }
        let mut range_begin_end=self.range_begin_end.lock().await;
        if range_begin_end.0==range_begin_end.1{
            // need update
            let ret=etcd::fetch_add_inode_next_range(
                // here appeared an await, so we need to use tokio::sync::Mutex
                Arc::clone(etcd_client),INODE_RANGE).await.unwrap_or_else(|e|{
                panic!("failed to fetch add inode next range, error is {:?}", e)
            });
            // begin
            range_begin_end.0=ret;
            // end
            range_begin_end.1=ret.overflow_add(INODE_RANGE-1);
            ret
        }else {
            let ret=range_begin_end.0;
            range_begin_end.0=range_begin_end.0.overflow_add(1);
            ret
        }
    }



    // Check the design at here ：https://github.com/datenlord/datenlord/issues/349
    //
    // One case is create new file
    // Another case is load uncached file from s3
    // The main point is to avoid two requests with same file path, but different inode number

    // Solution: use a global inode number for path, like "inode"+path: inum
    //  cases:
    //  1. communicate with other nodes, and got inum
    //  2. communicate and found no inum;  try write kv when there's none;  write success;
    //  3. communicate and found no inum;  try write kv when there's none;  write failed and get old;

    /// get a unique inum for a path when cache miss or creating a new file
    /// return (inum, is_new)
    async fn inode_get_inum_by_fullpath(&self,fullpath: &str,node_id:&str,volume_info:&str,etcd_client:&Arc<EtcdDelegate>) -> (INum,bool){
        //  1. communicate with other nodes and try to get existing inode info
        let fattr=dist::client::get_attr(
            Arc::clone(etcd_client),node_id,volume_info,fullpath).await.unwrap_or_else(|e|{
            panic!("failed to get attr, error is {:?}", e)
        });
        match fattr {
            None => {
                let inum=self.alloc_inum(etcd_client).await;
                let marked_inum=etcd::mark_fullpath_with_ino_in_etcd(etcd_client,&fullpath,inum).await.unwrap_or_else(|e|{
                    panic!("failed to mark fullpath with ino in etcd, error is {:?}", e)
                });
                if marked_inum==inum{// write success
                    (inum,true)
                }else{// write failed
                    // unused inum
                    self.recycle_unused.push(inum);
                    (marked_inum,false)
                }
            }
            Some(attr) => {
                (attr.ino,false)
            }
        }
    }
}

impl<S: S3BackEnd + Send + Sync + 'static> S3MetaData<S> {

    #[inline]
    pub(crate) async fn inode_get_inum_by_fullpath(&self,fullpath: &str) -> (INum,bool){
        self.inode_state.inode_get_inum_by_fullpath(fullpath,&self.node_id.as_str(),
                                   self.volume_info.as_str(),
                                   &self.etcd_client).await                                   
    }
}