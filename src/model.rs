use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HostId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RackId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PodId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ReplicaId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Active,
    Unload,
    Failed,
}

/// Physical host/machine.
#[derive(Debug, Clone)]
pub struct Host {
    pub id: HostId,
    pub name: String,
    pub rack_id: RackId,
}

/// Storage endpoint on a host (disk / storage node).
#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub host_id: HostId,
    pub rack_id: RackId,
    pub device_name: String,
    /// Used by balancers as a capacity/weight signal (e.g. GB).
    pub weight: u64,
    pub state: NodeState,
}

/// A shard is a slice of the keyspace (e.g. "volume" in O2 docs).
///
/// In this simplified model:
/// - Shard is assigned to a **pod**
/// - Shard contains multiple **replicas**
/// - Each replica is assigned to a **node** within that pod
#[derive(Debug, Clone)]
pub struct Shard {
    pub id: ShardId,
    pub replicas: Vec<ReplicaId>,
}

#[derive(Debug, Clone)]
pub struct Replica {
    pub id: ReplicaId,
    pub shard_id: ShardId,
    /// A human-friendly name; in O2 this is often derived from (shard, fragment).
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub num_shards: u32,
    pub replica_factor: u32,
    /// Target pod width in number of nodes (roughly `PodSizeTarget::NodeCount` in O2).
    pub target_pod_width: usize,
    /// Minimum number of spare nodes to keep unassigned to pods.
    pub min_spare_nodes: usize,
}

#[derive(Debug, Clone)]
pub struct Pod {
    pub id: PodId,
    pub nodes: HashSet<NodeId>,
    pub shards: HashSet<ShardId>,
}

/// Central mapping structure, mirroring O2's "pod mapping" idea:
/// - **node/device → pod**
/// - **shard → pod**
/// - **replica → node**
#[derive(Debug, Clone)]
pub struct PodMapping {
    pub version: u64,
    pub pods: BTreeMap<PodId, Pod>,
    pub spares: HashSet<NodeId>,

    pub node_to_pod: HashMap<NodeId, PodId>,
    pub shard_to_pod: HashMap<ShardId, PodId>,

    pub shard_to_replicas: HashMap<ShardId, Vec<ReplicaId>>,
    pub replica_to_shard: HashMap<ReplicaId, ShardId>,
    pub replica_to_node: HashMap<ReplicaId, NodeId>,
}

impl PodMapping {
    pub fn new() -> Self {
        Self {
            version: 0,
            pods: BTreeMap::new(),
            spares: HashSet::new(),
            node_to_pod: HashMap::new(),
            shard_to_pod: HashMap::new(),
            shard_to_replicas: HashMap::new(),
            replica_to_shard: HashMap::new(),
            replica_to_node: HashMap::new(),
        }
    }
}


