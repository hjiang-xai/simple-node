//! Simplified Otto: A 500-line Topology Manager
//!
//! This is a simplified version of Otto's topology management to help understand
//! the core concepts without the complexity of distributed systems, security,
//! and robustness considerations.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

// Pod balancer types (simplified version for demonstration)
#[derive(Debug, Clone)]
pub struct PodMapping {
    pub pods: HashMap<PodId, Pod>,
    pub unassigned_nodes: HashSet<NodeId>,
    pub version: u64,
}

#[derive(Debug, Clone)]
pub struct Pod {
    pub id: PodId,
    pub nodes: HashSet<NodeId>,
    pub total_capacity: u64,
    pub used_capacity: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PodId(u64);

impl PodMapping {
    pub fn new() -> Self {
        Self {
            pods: HashMap::new(),
            unassigned_nodes: HashSet::new(),
            version: 0,
        }
    }
}

pub struct PodBalancer;
pub struct DebouncedBalancer;

// Include shared types
include!("simplified_types.rs");

/// Represents a physical host/machine
#[derive(Debug, Clone)]
pub struct Host {
    pub id: HostId,
    pub name: String,
    pub rack: String,
    pub nodes: Vec<NodeId>,  // Nodes on this host
}

/// Represents the cluster topology
#[derive(Debug, Clone)]
pub struct Topology {
    pub version: u64,
    pub nodes: HashSet<NodeId>,
    pub hosts: HashSet<HostId>,
    pub racks: HashMap<String, HashSet<HostId>>,  // rack name -> hosts
}

impl Topology {
    pub fn new() -> Self {
        Self {
            version: 0,
            nodes: HashSet::new(),
            hosts: HashSet::new(),
            racks: HashMap::new(),
        }
    }

    pub fn add_host(&mut self, host_id: HostId, rack: String) {
        self.hosts.insert(host_id);
        self.racks.entry(rack).or_insert_with(HashSet::new).insert(host_id);
        self.version += 1;
    }

    pub fn add_node(&mut self, node_id: NodeId) {
        self.nodes.insert(node_id);
        self.version += 1;
    }

    pub fn remove_node(&mut self, node_id: NodeId) {
        self.nodes.remove(&node_id);
        self.version += 1;
    }

    pub fn hosts_in_rack(&self, rack: &str) -> Vec<HostId> {
        self.racks.get(rack)
            .map(|hosts| hosts.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn nodes_in_rack(&self, rack: &str, hosts: &HashMap<HostId, Host>) -> Vec<NodeId> {
        let mut nodes = Vec::new();
        if let Some(rack_hosts) = self.racks.get(rack) {
            for host_id in rack_hosts {
                if let Some(host) = hosts.get(host_id) {
                    nodes.extend(&host.nodes);
                }
            }
        }
        nodes
    }

}

/// Simplified load balancer
pub struct LoadBalancer {
    node_loads: HashMap<NodeId, u64>,
}

impl LoadBalancer {
    pub fn new() -> Self {
        Self {
            node_loads: HashMap::new(),
        }
    }

    pub fn update_loads(&mut self, nodes: &HashMap<NodeId, Node>) {
        self.node_loads.clear();
        for (node_id, node) in nodes {
            self.node_loads.insert(*node_id, node.used);
        }
    }

    pub fn select_node_for_load(&self, nodes: &HashMap<NodeId, Node>, load_size: u64) -> Option<NodeId> {
        // Find active node with most available capacity
        nodes.values()
            .filter(|node| node.state == NodeState::Active && node.available_capacity() >= load_size)
            .max_by_key(|node| node.available_capacity())
            .map(|node| node.id)
    }

    pub fn is_balanced(&self, nodes: &HashMap<NodeId, Node>) -> bool {
        let active_nodes: Vec<_> = nodes.values()
            .filter(|n| n.state == NodeState::Active)
            .collect();

        if active_nodes.len() <= 1 {
            return true;
        }

        let utilizations: Vec<f64> = active_nodes.iter()
            .map(|n| n.utilization_percent())
            .collect();

        let avg_util = utilizations.iter().sum::<f64>() / utilizations.len() as f64;
        let max_deviation = utilizations.iter()
            .map(|&util| (util - avg_util).abs())
            .fold(0.0, f64::max);

        // Consider balanced if max deviation < 20%
        max_deviation < 20.0
    }
}

/// The main Otto topology manager
///
/// # Pod Layer Integration
/// Otto manages topology at multiple levels:
/// 1. **Physical**: Hosts → Nodes (disks)
/// 2. **Logical**: Pods → Nodes (for VB assignment)
/// 3. **Data**: Virtual Buckets → Pods → Nodes
///
/// Pods sit between physical topology and data distribution:
/// - Pods group nodes for efficient rebalancing
/// - Virtual buckets are assigned to pods (not individual nodes)
/// - Node failures within a pod don't require VB reassignment
pub struct Otto {
    topology: Arc<RwLock<Topology>>,
    hosts: Arc<RwLock<HashMap<HostId, Host>>>,
    nodes: Arc<RwLock<HashMap<NodeId, Node>>>,
    balancer: LoadBalancer,
    pod_balancer: Option<DebouncedBalancer>,
    pod_mapping: Arc<RwLock<PodMapping>>,
    next_host_id: u64,
    next_node_id: u64,
}

impl Otto {
    pub fn new() -> Self {
        Self {
            topology: Arc::new(RwLock::new(Topology::new())),
            hosts: Arc::new(RwLock::new(HashMap::new())),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            balancer: LoadBalancer::new(),
            pod_balancer: None,
            pod_mapping: Arc::new(RwLock::new(PodMapping::new())),
            next_host_id: 1,
            next_node_id: 1,
        }
    }

    /// Enable pod balancing with specified number of pods and debounce time
    pub fn enable_pod_balancing(&mut self, num_pods: usize, debounce_secs: u64) {
        self.pod_balancer = Some(DebouncedBalancer::new(num_pods, debounce_secs));
    }

    /// Register a new host
    pub async fn register_host(&mut self, hostname: String, rack: String) -> HostId {
        let host_id = HostId(self.next_host_id);
        self.next_host_id += 1;

        let host = Host {
            id: host_id,
            name: hostname,
            rack: rack.clone(),
            nodes: Vec::new(),
        };

        // Add to hosts
        {
            let mut hosts = self.hosts.write().await;
            hosts.insert(host_id, host);
        }

        // Add to topology
        {
            let mut topology = self.topology.write().await;
            topology.add_host(host_id, rack);
        }

        host_id
    }

    /// Register a new node on an existing host
    pub async fn register_node(&mut self, host_id: HostId, device_name: String, capacity: u64) -> Result<NodeId, String> {
        // Check if host exists
        {
            let hosts = self.hosts.read().await;
            if !hosts.contains_key(&host_id) {
                return Err(format!("Host {} not found", host_id.0));
            }
        }

        let node_id = NodeId(self.next_node_id);
        self.next_node_id += 1;

        let node = Node {
            id: node_id,
            host_id,
            device_name,
            capacity,
            used: 0,
            state: NodeState::Active,
        };

        // Add to nodes
        {
            let mut nodes = self.nodes.write().await;
            nodes.insert(node_id, node);
        }

        // Add node to host
        {
            let mut hosts = self.hosts.write().await;
            if let Some(host) = hosts.get_mut(&host_id) {
                host.nodes.push(node_id);
            }
        }

        // Add to topology
        {
            let mut topology = self.topology.write().await;
            topology.add_node(node_id);
        }

        // Update balancer
        {
            let nodes = self.nodes.read().await;
            self.balancer.update_loads(&nodes);

            // Update pod mapping if enabled
            if let Some(ref mut pod_balancer) = self.pod_balancer {
                let mut pod_mapping = self.pod_mapping.write().await;
                if pod_mapping.pods.is_empty() {
                    // Create initial mapping if this is the first node
                    *pod_mapping = pod_balancer.create_initial_mapping(&nodes);
                } else {
                    // Try to assign new node to existing pods
                    let active_pods: Vec<_> = pod_mapping.pods.keys().cloned().collect();
                    if !active_pods.is_empty() {
                        let pod_id = active_pods[0]; // Simple: assign to first pod
                        pod_mapping.assign_node_to_pod(node_id, pod_id, capacity, 0);
                    }
                }
            }
        }

        Ok(node_id)
    }

    /// Update node state
    pub async fn update_node_state(&self, node_id: NodeId, state: NodeState) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(&node_id) {
            node.state = state;
        }

        // Update balancer with new state
        self.balancer.update_loads(&nodes);
    }

    /// Update node capacity usage
    pub async fn update_node_usage(&self, node_id: NodeId, used: u64) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(&node_id) {
            node.used = used.min(node.capacity);
        }

        // Update balancer with new usage
        self.balancer.update_loads(&nodes);
    }

    /// Remove a node
    pub async fn remove_node(&self, node_id: NodeId) {
        // Remove from host
        {
            let node_host_id = {
                let nodes = self.nodes.read().await;
                nodes.get(&node_id).map(|n| n.host_id)
            };

            if let Some(host_id) = node_host_id {
                let mut hosts = self.hosts.write().await;
                if let Some(host) = hosts.get_mut(&host_id) {
                    host.nodes.retain(|&id| id != node_id);
                }
            }
        }

        // Remove from nodes
        {
            let mut nodes = self.nodes.write().await;
            nodes.remove(&node_id);
            self.balancer.update_loads(&nodes);
        }

        // Remove from topology
        {
            let mut topology = self.topology.write().await;
            topology.remove_node(node_id);
        }
    }

    /// Select best node for new data
    pub async fn select_node_for_data(&self, data_size: u64) -> Option<NodeId> {
        let nodes = self.nodes.read().await;
        self.balancer.select_node_for_load(&nodes, data_size)
    }

    /// Check if cluster needs balancing
    pub async fn needs_balancing(&self) -> bool {
        let nodes = self.nodes.read().await;
        !self.balancer.is_balanced(&nodes)
    }

    /// Try to balance pods (with debouncing)
    pub async fn balance_pods(&mut self) -> bool {
        if let Some(ref mut pod_balancer) = self.pod_balancer {
            let nodes = self.nodes.read().await;
            let mut pod_mapping = self.pod_mapping.write().await;
            pod_balancer.try_balance(&mut pod_mapping, &nodes).await
        } else {
            false
        }
    }

    /// Get current pod mapping
    pub async fn get_pod_mapping(&self) -> PodMapping {
        self.pod_mapping.read().await.clone()
    }

    /// Check if pods need balancing
    pub async fn pods_need_balancing(&self) -> bool {
        if let Some(ref pod_balancer) = self.pod_balancer {
            let nodes = self.nodes.read().await;
            let pod_mapping = self.pod_mapping.read().await;

            // Check if any pod is overloaded (>80%) or underloaded (<20%)
            for pod in pod_mapping.pods.values() {
                let utilization = pod_mapping.pod_utilization_percent(&pod.id);
                if utilization > 80.0 || (utilization < 20.0 && !pod.nodes.is_empty()) {
                    return true;
                }
            }
        }
        false
    }



    /// Get cluster statistics
    pub async fn get_cluster_stats(&self) -> ClusterStats {
        let topology = self.topology.read().await;
        let hosts = self.hosts.read().await;
        let nodes = self.nodes.read().await;
        let pod_mapping = self.pod_mapping.read().await;

        let total_capacity: u64 = nodes.values().map(|n| n.capacity).sum();
        let active_nodes = nodes.values().filter(|n| n.state == NodeState::Active).count();
        let pods_enabled = self.pod_balancer.is_some();
        let pod_count = pod_mapping.pods.len();
        let pods_balanced = !self.pods_need_balancing().await;

        ClusterStats {
            total_hosts: hosts.len(),
            total_nodes: nodes.len(),
            active_nodes,
            total_capacity,
            topology_version: topology.version,
            is_balanced: self.balancer.is_balanced(&nodes),
            pods_enabled,
            pod_count,
            pods_balanced,
        }
    }
}

/// Cluster-wide statistics
#[derive(Debug, Clone)]
pub struct ClusterStats {
    pub total_hosts: usize,
    pub total_nodes: usize,
    pub active_nodes: usize,
    pub total_capacity: u64,
    pub topology_version: u64,
    pub is_balanced: bool,
    pub pods_enabled: bool,
    pub pod_count: usize,
    pub pods_balanced: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_host_node_operations() {
        let mut otto = Otto::new();

        // Register a host
        let host_id = otto.register_host("host1".to_string(), "rack-1".to_string()).await;

        // Register a node on that host
        let node_id = otto.register_node(host_id, "sda1".to_string(), 1000).await.unwrap();

        let hosts = otto.get_hosts().await;
        assert_eq!(hosts.len(), 1);

        let nodes = otto.get_nodes().await;
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[&node_id].host_id, host_id);
    }

    #[tokio::test]
    async fn test_node_selection() {
        let mut otto = Otto::new();
        let host = otto.register_host("host".to_string(), "rack-1".to_string()).await;
        let node = otto.register_node(host, "sda".to_string(), 1000).await.unwrap();

        let selected = otto.select_node_for_data(500).await;
        assert_eq!(selected, Some(node));
    }

    #[tokio::test]
    async fn test_cluster_stats() {
        let mut otto = Otto::new();
        let host = otto.register_host("host".to_string(), "rack".to_string()).await;
        otto.register_node(host, "sda".to_string(), 1000).await.unwrap();

        let stats = otto.get_cluster_stats().await;
        assert_eq!(stats.total_hosts, 1);
        assert_eq!(stats.total_nodes, 1);
        assert!(!stats.pods_enabled);
        assert_eq!(stats.pod_count, 0);
    }
}
