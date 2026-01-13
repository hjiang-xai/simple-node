use std::collections::HashMap;

use crate::model::{
    ClusterConfig, Host, HostId, Node, NodeId, NodeState, PodMapping, RackId, Replica, ReplicaId, Shard, ShardId,
};
use crate::pod_balancer::{BalanceResult, PodBalancer};

/// In-memory "Otto" that owns:
/// - roster of hosts/nodes (physical topology)
/// - shard/replica inventory (data topology)
/// - pod mapping (logical topology)
///
/// This mirrors the key relationship in the real O2 system:
/// `Shard -> Pod -> Nodes` and `Replica -> Node`, with rack/host metadata driving constraints.
#[derive(Debug)]
pub struct Otto {
    cfg: ClusterConfig,

    next_host_id: u32,
    next_rack_id: u32,
    next_node_id: u32,
    next_replica_id: u32,

    rack_name_to_id: HashMap<String, RackId>,

    pub hosts: HashMap<HostId, Host>,
    pub nodes: HashMap<NodeId, Node>,

    pub shards: HashMap<ShardId, Shard>,
    pub replicas: HashMap<ReplicaId, Replica>,

    pub mapping: PodMapping,
    balancer: PodBalancer,
}

impl Otto {
    pub fn new(cfg: ClusterConfig) -> Self {
        Self {
            balancer: PodBalancer::new(cfg.clone()),
            cfg,
            next_host_id: 0,
            next_rack_id: 0,
            next_node_id: 0,
            next_replica_id: 0,
            rack_name_to_id: HashMap::new(),
            hosts: HashMap::new(),
            nodes: HashMap::new(),
            shards: HashMap::new(),
            replicas: HashMap::new(),
            mapping: PodMapping::new(),
        }
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.cfg
    }

    pub fn register_host(&mut self, hostname: impl Into<String>, rack_name: impl Into<String>) -> HostId {
        let hostname = hostname.into();
        let rack_name = rack_name.into();
        let rack_id = self.get_or_create_rack_id(&rack_name);

        let host_id = HostId(self.next_host_id);
        self.next_host_id += 1;

        self.hosts.insert(
            host_id,
            Host {
                id: host_id,
                name: hostname,
                rack_id,
            },
        );
        host_id
    }

    pub fn register_node(&mut self, host_id: HostId, device_name: impl Into<String>, weight: u64) -> Result<NodeId, String> {
        let host = self
            .hosts
            .get(&host_id)
            .ok_or_else(|| format!("host {host_id:?} not found"))?
            .clone();

        let node_id = NodeId(self.next_node_id);
        self.next_node_id += 1;

        self.nodes.insert(
            node_id,
            Node {
                id: node_id,
                host_id,
                rack_id: host.rack_id,
                device_name: device_name.into(),
                weight,
                state: NodeState::Active,
            },
        );

        Ok(node_id)
    }

    pub fn mark_node_state(&mut self, node_id: NodeId, state: NodeState) -> Result<(), String> {
        let node = self.nodes.get_mut(&node_id).ok_or_else(|| format!("node {node_id:?} not found"))?;
        node.state = state;
        Ok(())
    }

    pub fn ensure_shards(&mut self) {
        while self.shards.len() < self.cfg.num_shards as usize {
            let shard_id = ShardId(self.shards.len() as u32);
            let mut replica_ids = Vec::with_capacity(self.cfg.replica_factor as usize);
            for r in 0..self.cfg.replica_factor {
                let replica_id = ReplicaId(self.next_replica_id);
                self.next_replica_id += 1;
                let replica = Replica {
                    id: replica_id,
                    shard_id,
                    name: format!("shard{}-r{}", shard_id.0, r),
                };
                self.replicas.insert(replica_id, replica);
                replica_ids.push(replica_id);
            }
            self.shards.insert(shard_id, Shard { id: shard_id, replicas: replica_ids });
        }
    }

    pub fn rebalance(&mut self) -> BalanceResult {
        self.ensure_shards();
        let rack_of_node: HashMap<NodeId, RackId> = self.nodes.iter().map(|(&id, n)| (id, n.rack_id)).collect();
        self.balancer
            .instantly_balance(&mut self.mapping, &self.nodes, &self.shards, &rack_of_node)
    }

    pub fn shard_assignments(&self) -> Vec<(ShardId, Option<crate::model::PodId>)> {
        let mut v: Vec<_> = self
            .shards
            .keys()
            .copied()
            .map(|sid| (sid, self.mapping.shard_to_pod.get(&sid).copied()))
            .collect();
        v.sort_by_key(|(sid, _)| sid.0);
        v
    }

    pub fn replica_assignments_for_shard(&self, shard_id: ShardId) -> Vec<(ReplicaId, Option<NodeId>)> {
        let mut v = Vec::new();
        if let Some(shard) = self.shards.get(&shard_id) {
            for &replica_id in &shard.replicas {
                v.push((replica_id, self.mapping.replica_to_node.get(&replica_id).copied()));
            }
        }
        v
    }

    fn get_or_create_rack_id(&mut self, rack_name: &str) -> RackId {
        if let Some(&id) = self.rack_name_to_id.get(rack_name) {
            return id;
        }
        let id = RackId(self.next_rack_id);
        self.next_rack_id += 1;
        self.rack_name_to_id.insert(rack_name.to_string(), id);
        id
    }
}


