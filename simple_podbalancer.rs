//! Simplified Pod Balancer and Debounce Logic
//!
//! This complements simplified_otto.rs with pod balancing and debouncing capabilities.
//! Keeps logic simple and straightforward for understanding.
//!
//! ## Pod vs Virtual Bucket/Partition Relationship
//!
//! ### Pods (in toy implementation):
//! - **What**: Logical grouping of storage nodes for load balancing and fault isolation
//! - **Purpose**: Contain blast radius during rebalancing, improve rebalance efficiency
//! - **Contains**: Multiple nodes (disks) from different hosts
//! - **Example**: Pod P1 contains nodes [N1, N2, N3] from hosts [H1, H2]
//!
//! ### Virtual Buckets/Partitions (conceptual - not implemented in toy):
//! - **What**: Logical units of data distribution (like shards or partitions)
//! - **Purpose**: Each object belongs to exactly one VB, VBs are distributed across pods
//! - **Mapping**: VB → Pod → Nodes (each VB's copies are stored in one pod)
//! - **Example**: VB 1000 → Pod P1 → stored on nodes N1, N2, N3 within P1
//!
//! ### Why This Separation Matters:
//! 1. **Fault Isolation**: Node failure in Pod P1 only affects VBs in P1 (not all VBs)
//! 2. **Rebalance Efficiency**: Moving VBs between pods moves data in pod-sized chunks
//! 3. **Load Distribution**: VBs balanced across pods, pods balanced across nodes
//! 4. **Replication**: Multiple copies of each VB exist in same pod (different nodes)
//!
//! ### Data Flow:
//! Object Key → Hash → VB ID → Pod ID → Node IDs (where VB replicas are stored)
//!
//! ### In Real O2:
//! - ~1000 VBs total
//! - ~36 nodes per pod
//! - RF=2 (2 copies per VB, both in same pod but different racks)
//! - Pod balancer computes VB → Pod mappings

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// A logical grouping of nodes (like pods in real Otto)
///
/// # Relationship to Virtual Buckets
/// In the real system, pods serve virtual buckets (VBs):
/// - Each VB belongs to exactly one pod
/// - All replicas of a VB are stored within the same pod (different nodes)
/// - Pod balancer decides which pod gets which VBs
/// - Node balancer within pod decides which nodes get which VBs
///
/// In this toy implementation, we balance nodes between pods,
/// but don't implement the VB → Pod → Node mapping layer.
#[derive(Debug, Clone)]
pub struct Pod {
    pub id: PodId,
    pub nodes: HashSet<NodeId>,      // Nodes assigned to this pod
    pub total_capacity: u64,         // Sum of all node capacities in pod
    pub used_capacity: u64,          // Sum of all node usage in pod
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PodId(u64);

/// Simplified pod mapping (equivalent to PodMapping in real Otto)
#[derive(Debug, Clone)]
pub struct PodMapping {
    pub pods: HashMap<PodId, Pod>,
    pub unassigned_nodes: HashSet<NodeId>,
    pub version: u64,
}

impl PodMapping {
    pub fn new() -> Self {
        Self {
            pods: HashMap::new(),
            unassigned_nodes: HashSet::new(),
            version: 0,
        }
    }

    pub fn add_pod(&mut self, pod_id: PodId) {
        let pod = Pod {
            id: pod_id,
            nodes: HashSet::new(),
            total_capacity: 0,
            used_capacity: 0,
        };
        self.pods.insert(pod_id, pod);
    }

    pub fn assign_node_to_pod(&mut self, node_id: NodeId, pod_id: PodId, capacity: u64, used: u64) {
        if let Some(pod) = self.pods.get_mut(&pod_id) {
            pod.nodes.insert(node_id);
            pod.total_capacity += capacity;
            pod.used_capacity += used;
        }
        self.unassigned_nodes.remove(&node_id);
        self.version += 1;
    }

    pub fn remove_node_from_pod(&mut self, node_id: NodeId) {
        for pod in self.pods.values_mut() {
            if pod.nodes.remove(&node_id) {
                // Note: In real implementation, we'd track per-node capacity
                // For simplicity, we don't update totals here
                break;
            }
        }
        self.version += 1;
    }

    pub fn get_pod_for_node(&self, node_id: &NodeId) -> Option<PodId> {
        for (pod_id, pod) in &self.pods {
            if pod.nodes.contains(node_id) {
                return Some(*pod_id);
            }
        }
        None
    }

    pub fn pod_utilization_percent(&self, pod_id: &PodId) -> f64 {
        if let Some(pod) = self.pods.get(pod_id) {
            if pod.total_capacity == 0 {
                0.0
            } else {
                (pod.used_capacity as f64 / pod.total_capacity as f64) * 100.0
            }
        } else {
            0.0
        }
    }
}

/// Simplified pod balancer (equivalent to PodBalancer in real Otto)
///
/// # Virtual Bucket Context
/// In real Otto, this balancer would:
/// 1. Take VB → Pod assignments from podbalancer library
/// 2. Ensure pods have capacity for their assigned VBs
/// 3. Move VBs between pods when nodes fail/become overloaded
///
/// Our toy version just balances nodes between pods based on capacity,
/// assuming VBs would follow the node movements.
///
/// # Conceptual VB → Pod → Node Mapping Example:
/// ```text
/// Virtual Buckets:  VB1000, VB1001, VB1002, ...
///
/// Pod Mapping:      VB1000 → Pod1, VB1001 → Pod1, VB1002 → Pod2, ...
///
/// Node Assignment:  Pod1 → [Node1, Node2, Node3]
///                   Pod2 → [Node4, Node5, Node6]
///
/// Data Storage:     VB1000 replicas → Node1, Node2 (in Pod1)
///                   VB1001 replicas → Node2, Node3 (in Pod1)
///                   VB1002 replicas → Node4, Node5 (in Pod2)
/// ```
pub struct PodBalancer {
    num_pods: usize,
    next_pod_id: u64,
}

impl PodBalancer {
    pub fn new(num_pods: usize) -> Self {
        Self {
            num_pods,
            next_pod_id: 1,
        }
    }

    /// Create initial pod mapping from nodes
    pub fn create_initial_mapping(&mut self, nodes: &HashMap<NodeId, Node>) -> PodMapping {
        let mut mapping = PodMapping::new();

        // Create pods
        for i in 0..self.num_pods {
            let pod_id = PodId(self.next_pod_id + i as u64);
            mapping.add_pod(pod_id);
        }
        self.next_pod_id += self.num_pods as u64;

        // Assign nodes to pods (simple round-robin)
        let active_nodes: Vec<_> = nodes.values()
            .filter(|n| n.state == NodeState::Active)
            .collect();

        for (i, node) in active_nodes.iter().enumerate() {
            let pod_index = i % self.num_pods;
            let pod_id = PodId(1 + pod_index as u64);
            mapping.assign_node_to_pod(node.id, pod_id, node.capacity, node.used);
        }

        mapping
    }

    /// Balance nodes across pods based on capacity
    pub fn balance(&mut self, mapping: &mut PodMapping, nodes: &HashMap<NodeId, Node>) {
        // Simple balancing: move nodes from overloaded pods to underloaded pods
        let mut overloaded_nodes = Vec::new();

        // Find overloaded pods (utilization > 80%)
        for pod in mapping.pods.values() {
            let utilization = mapping.pod_utilization_percent(&pod.id);
            if utilization > 80.0 {
                // Find largest nodes in this pod to potentially move
                for &node_id in &pod.nodes {
                    if let Some(node) = nodes.get(&node_id) {
                        if node.state == NodeState::Active {
                            overloaded_nodes.push((node_id, node.used));
                        }
                    }
                }
            }
        }

        // Sort by size (largest first)
        overloaded_nodes.sort_by(|a, b| b.1.cmp(&a.1));

        // Try to move largest nodes to underloaded pods
        for (node_id, _) in overloaded_nodes {
            if let Some(current_pod) = mapping.get_pod_for_node(&node_id) {
                // Find underloaded pod with capacity
                for (pod_id, pod) in &mapping.pods {
                    if *pod_id != current_pod {
                        let utilization = mapping.pod_utilization_percent(pod_id);
                        if utilization < 60.0 {
                            // Move node to this pod
                            mapping.remove_node_from_pod(node_id);
                            if let Some(node) = nodes.get(&node_id) {
                                mapping.assign_node_to_pod(node_id, *pod_id, node.capacity, node.used);
                            }
                            break;
                        }
                    }
                }
            }
        }
    }
}

/// Debounce state for topology changes
#[derive(Debug)]
struct DebounceState {
    last_topology_key: Option<TopologyKey>,
    last_change_time: Option<Instant>,
}

/// Key representing topology state for debouncing
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TopologyKey {
    node_count: usize,
    total_capacity: u64,
    active_nodes: usize,
}

/// Simplified debounced balancer
pub struct DebouncedBalancer {
    inner: PodBalancer,
    debounce_duration: Duration,
    debounce_state: Arc<RwLock<DebounceState>>,
}

impl DebouncedBalancer {
    pub fn new(num_pods: usize, debounce_secs: u64) -> Self {
        Self {
            inner: PodBalancer::new(num_pods),
            debounce_duration: Duration::from_secs(debounce_secs),
            debounce_state: Arc::new(RwLock::new(DebounceState {
                last_topology_key: None,
                last_change_time: None,
            })),
        }
    }

    /// Create initial mapping (no debouncing needed)
    pub fn create_initial_mapping(&mut self, nodes: &HashMap<NodeId, Node>) -> PodMapping {
        self.inner.create_initial_mapping(nodes)
    }

    /// Try to balance, but only if topology has been stable
    pub async fn try_balance(&mut self, mapping: &mut PodMapping, nodes: &HashMap<NodeId, Node>) -> bool {
        // Check if enough time has passed since last change
        if let Some(change_time) = debounce_state.last_change_time {
            if change_time.elapsed() < self.debounce_duration {
                return false; // Still in debounce period
            }
        }

        let current_key = TopologyKey {
            node_count: nodes.len(),
            total_capacity: nodes.values().map(|n| n.capacity).sum(),
            active_nodes: nodes.values().filter(|n| n.state == NodeState::Active).count(),
        };

        let mut debounce_state = self.debounce_state.write().await;

        // Check if topology changed
        if Some(&current_key) != debounce_state.last_topology_key.as_ref() {
            // Topology changed, reset debounce timer
            debounce_state.last_topology_key = Some(current_key);
            debounce_state.last_change_time = Some(Instant::now());
            return false; // Don't balance yet
        }
        
        // Topology is stable, perform balancing
        self.inner.balance(mapping, nodes);
        true // Balancing was performed
    }
}

// Include shared types
include!("simplified_types.rs");

impl Node {
    pub fn utilization_percent(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            (self.used as f64 / self.capacity as f64) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pod_balancer_basic() {
        let mut balancer = PodBalancer::new(2);

        // Create some test nodes
        let mut nodes = HashMap::new();
        nodes.insert(NodeId(1), Node::new(NodeId(1), HostId(1), "sda".to_string(), 1000));
        nodes.insert(NodeId(2), Node::new(NodeId(2), HostId(1), "sdb".to_string(), 1000));

        // Simulate load
        nodes.get_mut(&NodeId(1)).unwrap().used = 900; // 90%
        nodes.get_mut(&NodeId(2)).unwrap().used = 100; // 10%

        let mut mapping = balancer.create_initial_mapping(&nodes);

        // Should have 2 pods
        assert_eq!(mapping.pods.len(), 2);

        // Each pod should have 1 node
        for pod in mapping.pods.values() {
            assert_eq!(pod.nodes.len(), 1);
        }
    }

    #[tokio::test]
    async fn test_debounced_balancer() {
        let mut debounced = DebouncedBalancer::new(1, 1); // 1 second debounce

        // Create test nodes
        let mut nodes = HashMap::new();
        nodes.insert(NodeId(1), Node::new(NodeId(1), HostId(1), "sda".to_string(), 1000));

        let mut mapping = debounced.create_initial_mapping(&nodes);

        // First attempt should be debounced
        let balanced = debounced.try_balance(&mut mapping, &nodes).await;
        assert!(!balanced);

        // Wait for debounce
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Second attempt should work
        let balanced = debounced.try_balance(&mut mapping, &nodes).await;
        assert!(balanced);
    }
}
