use std::{sync::Arc, time::Duration, collections::{HashMap, VecDeque}};
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::atomic::Ordering::Acquire;
use anyhow::{anyhow, Error};
use log::debug;

use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::async_fuse::fuse::protocol::INum;
use crate::async_fuse::memfs::s3_wrapper::S3BackEnd;
use serde::{Deserialize, Serialize};
use crate::async_fuse::fuse::file_system::FsAsyncResultSender;
use crate::async_fuse::memfs::dir::DirEntry;
use crate::async_fuse::memfs::inode::InodeState;
use crate::async_fuse::memfs::s3_node::S3NodeData;
use crate::async_fuse::memfs::S3MetaData;
use super::fs_util::FileAttr;

const DIR_DATA_KEY_PREFIX:&str="dir_";
const PERSIST_RETRY_TIMES:u32=3;

#[derive(Debug)]
enum PersistError{
    S3Error,
}

// read when child dir cache missed in lookup
//  or open_root_node when start.
pub(crate) async fn read_persisted_dir<S: S3BackEnd + Sync + Send + 'static>(
    s3_backend: &Arc<S>,
    dir_full_path: String) -> anyhow::Result<PersistDirContent> {
    let res=s3_backend.get_data(&*format!("{DIR_DATA_KEY_PREFIX}{dir_full_path}")).await?;// return if not exist
    match PersistDirContent::new_from_store(dir_full_path,res.as_slice()){
        Ok(dir_content)=>Ok(dir_content),
        Err(e)=>{
            debug!("failed to deserialize dir data, {e}");
            Err(e)
        }
    }
}
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct PersistSerialized{
    pub(crate) file_map:HashMap<String,FileAttr>,// child file name to attr
    pub(crate) root_attr:Option<FileAttr>
}
pub(crate) struct PersistDirContent{
    pub(crate) dir_full_path:String,
    pub(crate) persist_serialized:PersistSerialized
}
impl PersistDirContent{
    pub(crate) async fn new_s3_node_data_dir(&self) -> S3NodeData
        where S : S3BackEnd + Send + Sync + 'static
    {
        // we need to allocate new inums for files in it.
        // inum for file is needed because many place called entry.ino()

        let mut dir_map=
            self.persist_serialized.file_map.iter().map(|(name,fattr)|{
                (name.clone(),DirEntry::new(name.clone(), fattr.clone()))
        }).collect();
        S3NodeData::Directory(dir_map)
    }
    // pub(crate) fn new() -> PersistDirContent {
    //     PersistDirContent{
    //         dir_full_path:String::new(),
    //         file_map:HashMap::new()
    //     }
    // }
    fn new_from_store(dir_full_path:String, file_map_data:&[u8]) -> anyhow::Result<PersistDirContent> {
        let persist_serialized=bincode::deserialize(file_map_data)?;
        let new=PersistDirContent{ dir_full_path, persist_serialized};
        Ok(new)
    }
    fn serialize(&self)->Vec<u8>{
        bincode::serialize(&self.persist_serialized)
            .unwrap_or_else(|e| panic!("fail to serialize `PersistDirContent.file_map`, {e}"))
    }
}

struct PersistSharedState{
    dirty_map:RwLock<HashMap<INum,(VecDeque<Notify>,PersistDirContent)>>,
    system_end:AtomicBool,
}

pub(crate) struct PersistHandle{
    shared:Arc<PersistSharedState>,
    join_handle:JoinHandle<()>,
}
impl PersistHandle{
    fn new(shared:Arc<PersistSharedState>,join_handle:JoinHandle<()>)->PersistHandle{
        PersistHandle{
            shared,
            join_handle,
        }
    }
    pub(crate) async fn wait_persist(&self, inum:INum){
        // check if persisted
        // if not, regist notify and wait
        let notify =self.shared.dirty_map.read().get_mut(&inum)
            .map(|(notify_queue,_)|{
            let notify =Notify::new();
            notify_queue.push_back(notify.clone());
            notify
        });
        if let Some(notify)=notify{
            notify.notified().await;
        }
    }
    pub(crate) fn mark_dirty(&self,inum:INum, data_clone:PersistDirContent){
        let mut dirty_map_locked =self.shared.dirty_map.write();
        dirty_map_locked.entry(inum).or_insert_with(||(VecDeque::new(),data_clone));
    }
    #[inline]
    pub(crate) async fn system_end(&self){
        self.shared.system_end.store(true, Ordering::SeqCst);
        self.join_handle.await.unwrap_or_else(|e|{
            panic!("persist thread end panic, {e}");
        });
    }
}

pub(crate) struct PersistTask<S: S3BackEnd + Sync + Send + 'static>{
    shared:Arc<PersistSharedState>,
    s3_backend:Arc<S>
}

impl<S: S3BackEnd + Sync + Send + 'static> PersistTask<S> {
    #[inline]
    async fn try_persist_one_dirty(&self,fs_async_result_sender:&FsAsyncResultSender)->bool{
        let mut to_persist =None;
        let mut left_more_dirty =false;
        {// takeout one dirty data
            let mut dirty_map_locked =self.shared.dirty_map.write();
            let inum=dirty_map_locked.iter().next().map(|(inum,_)|*inum);
            if let Some(inum)=inum{
                let (notify_list,data)=dirty_map_locked.remove(&inum).unwrap();
                to_persist=Some((inum,notify_list,data));
                left_more_dirty=!dirty_map_locked.is_empty();
            }
        }
        if let Some((_,waiting_list,dirdata))=to_persist{
            // Do persist


            // Handle backend failure
            //  retry times then send error to main thread
            for i in 0..PERSIST_RETRY_TIMES {
                if let Err(e)= self.s3_backend.put_data(&*format!("{DIR_DATA_KEY_PREFIX}{}",
                                                                  dirdata.dir_full_path),
                                         &*dirdata.serialize()).await {
                    debug!("Failed to persist dir data, retried {i+1}/{PERSIST_RETRY_TIMES} times, {e:#?}");
                    if i==PERSIST_RETRY_TIMES-1{
                        // send err to main thread
                        fs_async_result_sender.send(Err(anyhow::Error::from(e))).await.unwrap_or_else(||{
                            debug!("Failed to send error to main thread");
                        });
                    }
                }else{
                    break;
                }
            }
            // Notify waiting operations
            for notify in &waiting_list{
                notify.notify_one();
            }
        }
        left_more_dirty
    }
    pub(crate) fn spawn(s3_backend:Arc<S>,fs_async_result_sender:FsAsyncResultSender) -> (PersistHandle)
        where
            T: Future + Send + 'static,
            T::Output: Send + 'static,
    {
        let shared =Arc::new(PersistSharedState{
            dirty_map: RwLock::default(),
            system_end: AtomicBool::new(false),
        });
        PersistHandle::new(Arc::clone(&shared),tokio::spawn(async {
            let fs_async_result_sender=fs_async_result_sender;
            let taskstate=PersistTask{
                shared,
                s3_backend,
            };
            //lazy scan for dirty data
            loop{
                // persist dirty until there's no dirty data
                while taskstate.try_persist_one_dirty(&fs_async_result_sender) {}
                if shared.system_end.load(Acquire){
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                if shared.system_end.load(Acquire){
                    break;
                }
            }
            debug!("persist task end");
        }))
    }
}