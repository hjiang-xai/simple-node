use std::collections::HashMap;

use crate::model::{HostId, NodeId};
use crate::otto::Otto;

/// Very small "storage node agent" that registers a host + its local disks into Otto.
///
/// In the real O2 system, storagenode is intentionally dumb:
/// it exposes a KV-ish interface and receives **volume/shard assignments** from Otto.
#[derive(Debug)]
pub struct StorageNodeAgent {
    pub hostname: String,
    pub rack: String,
    pub host_id: Option<HostId>,
    /// Local disk index -> cluster node id
    pub local_disks: HashMap<u32, NodeId>,
}

impl StorageNodeAgent {
    pub fn new(hostname: impl Into<String>, rack: impl Into<String>) -> Self {
        Self {
            hostname: hostname.into(),
            rack: rack.into(),
            host_id: None,
            local_disks: HashMap::new(),
        }
    }

    pub fn initialize(&mut self, otto: &mut Otto, disks: Vec<(u32, String, u64)>) -> Result<(), String> {
        let host_id = otto.register_host(self.hostname.clone(), self.rack.clone());
        self.host_id = Some(host_id);

        for (local_id, dev, weight) in disks {
            let node_id = otto.register_node(host_id, dev, weight)?;
            self.local_disks.insert(local_id, node_id);
        }
        Ok(())
    }
}


