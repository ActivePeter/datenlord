use std::{sync::Arc, time::Duration, collections::{HashMap, VecDeque}};

use parking_lot::RwLock;
use tokio::sync::Notify;

use crate::async_fuse::fuse::protocol::INum;

use super::fs_util::FileAttr;

struct PersistDirContent{
    file_map:HashMap<String,FileAttr>
}
impl PersistDirContent{
}

struct PersistSharedState{
    dirty_map:RwLock<HashMap<INum,(VecDeque<Notify>,PersistDirContent)>>
}

struct PersistHandle{
    shared:Arc<PersistSharedState>
}
impl PersistHandle{
    fn new(shared:Arc<PersistSharedState>)->PersistHandle{
        PersistHandle{
            shared:Arc::new(shared)
        }
    }
    fn wait_persist(&self,inum:INum){
        // check if persisted
        // if not regist notify and wait
    }
    fn mark_dirty(&self,inum:INum, data_clone:PersistDirContent){

    }
}

struct PersistTask{
    shared:Arc<PersistSharedState>
}

impl PersistTask{
    #[inline]
    fn try_persist_one_dirty()->bool{

    }
    fn spawn(){
        let shared=PersistSharedState{
            dirty_map: RwLock::default(),
        };
        PersistHandle::new(Arc::clone(shared));
        tokio::spawn(async {
            //lazy scan for dirty data
            loop{
                while try_persist_one_dirty() {
                    
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    }
}