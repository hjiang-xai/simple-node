//! Simplified StorageNode: A 500-line Host Storage Manager
//!
//! This simplified StorageNode shows how a host manages multiple storage nodes
//! and coordinates with Otto for cluster operations.
//!
//! Uses data structures from simplified_otto.rs

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Local disk identifier on a host (sequential: 0, 1, 2, ...)
pub type LocalDiskId = u32;

/// Simplified chunk/block operations using Otto's Node structures
pub struct ChunkService {
    hostname: String,
    otto: Arc<simplified_otto::Otto>,
    /// Mapping from local disk identifiers to Otto's global node identifiers.
    ///
    /// Why this mapping? StorageNode discovers disks locally (assigns sequential IDs: 0, 1, 2...)
    /// but Otto assigns global unique NodeIds to each storage endpoint. This mapping translates
    /// between local host-specific disk enumeration and Otto's cluster-wide node identification.
    ///
    /// Example: Local disk #2 on host "server01" → Otto's NodeId "abcd-1234-..."
    local_nodes: HashMap<LocalDiskId, simplified_otto::NodeId>,
}

impl ChunkService {
    pub fn new(hostname: String, otto: Arc<simplified_otto::Otto>, local_nodes: HashMap<LocalDiskId, simplified_otto::NodeId>) -> Self {
        Self { hostname, otto, local_nodes }
    }

    /// Read a chunk from specified node (disk)
    pub async fn read_chunk(&self, disk_id: LocalDiskId, chunk_id: u64, offset: u32, length: u32) -> Result<Vec<u8>, String> {
        let node_id = self.local_nodes.get(&disk_id)
            .ok_or_else(|| format!("Node {} not found on host {}", disk_id, self.hostname))?;

        let nodes = self.otto.get_nodes().await;
        let node = nodes.get(node_id)
            .ok_or_else(|| format!("Node {:?} not found in Otto", node_id))?;

        // Check if node is healthy
        if node.state != simplified_otto::NodeState::Active {
            return Err(format!("Node {:?} is not healthy", node_id));
        }

        // Simulate reading data (in real impl, this would read from actual disk)
        let data = vec![42u8; length as usize]; // Dummy data
        Ok(data)
    }

    /// Write a chunk to specified node (disk)
    pub async fn write_chunk(&self, disk_id: LocalDiskId, chunk_id: u64, offset: u32, data: Vec<u8>) -> Result<(), String> {
        let node_id = self.local_nodes.get(&disk_id)
            .ok_or_else(|| format!("Node {} not found on host {}", disk_id, self.hostname))?;

        let nodes = self.otto.get_nodes().await;
        let node = nodes.get(node_id)
            .ok_or_else(|| format!("Node {:?} not found in Otto", node_id))?;

        // Check if node is healthy and has capacity
        if node.state != simplified_otto::NodeState::Active {
            return Err(format!("Node {:?} is not healthy", node_id));
        }

        let new_usage = node.used + data.len() as u64;
        if new_usage > node.capacity {
            return Err("Write would exceed node capacity".to_string());
        }

        // Update usage in Otto
        self.otto.update_node_usage(*node_id, new_usage).await;
        Ok(())
    }

    /// Delete a chunk from specified node
    pub async fn delete_chunk(&self, disk_id: LocalDiskId, chunk_id: u64) -> Result<(), String> {
        // In real implementation, this would mark chunks as free
        // For simplicity, we don't track individual chunks
        Ok(())
    }

    /// Get node information from Otto
    pub async fn get_node_info(&self, disk_id: LocalDiskId) -> Result<simplified_otto::Node, String> {
        let node_id = self.local_nodes.get(&disk_id)
            .ok_or_else(|| format!("Node {} not found", disk_id))?;

        let nodes = self.otto.get_nodes().await;
        nodes.get(node_id)
            .cloned()
            .ok_or_else(|| format!("Node {:?} not found in Otto", node_id))
    }
}

/// The main StorageNode that manages a host
pub struct StorageNode {
    hostname: String,
    otto: Arc<simplified_otto::Otto>,  // Reference to Otto
    chunk_service: ChunkService,
    /// Maps local disk identifiers to Otto's global node identifiers.
    ///
    /// LocalDiskId (0,1,2...): How this StorageNode identifies disks on its host
    /// Otto's NodeId: Global unique identifier assigned by Otto for cluster coordination
    ///
    /// This translation table allows StorageNode to:
    /// 1. Receive requests using local disk IDs
    /// 2. Look up corresponding Otto NodeIds
    /// 3. Interact with Otto's global node registry
    registered_nodes: HashMap<LocalDiskId, simplified_otto::NodeId>,
}

impl StorageNode {
    pub fn new(hostname: String, otto: Arc<simplified_otto::Otto>) -> Self {
        let registered_nodes = HashMap::new(); // Will be populated during init
        let chunk_service = ChunkService::new(hostname.clone(), otto.clone(), registered_nodes.clone());

        Self {
            hostname,
            otto,
            chunk_service,
            registered_nodes,
        }
    }

    /// Initialize the storage node
    pub async fn initialize(&mut self) -> Result<(), String> {
        println!("🔧 Initializing StorageNode for host: {}", self.hostname);

        // Step 1: Register host with Otto
        let host_id = self.otto.register_host(
            self.hostname.clone(),
            "default-rack".to_string() // In real impl, this would be detected
        ).await;
        println!("✅ Registered host {} with Otto (host_id: {})", self.hostname, host_id.0);

        // Step 2: Simulate detecting local disks and register as nodes
        // In real impl, this would scan /dev/disk* and detect actual disks
        let simulated_disks = vec![
            (0, "sda1", 1000 * 1024 * 1024 * 1024), // 1TB
            (1, "sdb1", 1000 * 1024 * 1024 * 1024), // 1TB
            (2, "sdc1", 1000 * 1024 * 1024 * 1024), // 1TB
       ];

        println!("🔍 Detected {} storage devices on host {}", simulated_disks.len(), self.hostname);

        // Step 3: Register each storage device as a node with Otto
        // This creates the mapping: LocalDiskId → Otto's global NodeId
        for (disk_id, device_name, capacity) in simulated_disks {
            let node_id = self.otto.register_node(
                host_id,
                device_name.to_string(),
                capacity,
            ).await
            .map_err(|e| format!("Failed to register node: {}", e))?;

            // Store the mapping: local disk ID → global Otto node ID
            self.registered_nodes.insert(disk_id, node_id);
            println!("✅ Registered {} (local disk #{}) as node {} with Otto",
                    device_name, disk_id, node_id.0);
        }

        // Update chunk service with registered nodes
        self.chunk_service = ChunkService::new(
            self.hostname.clone(),
            self.otto.clone(),
            self.registered_nodes.clone()
        );

        println!("🚀 StorageNode {} fully initialized with {} nodes", self.hostname, self.registered_nodes.len());
        Ok(())
    }

    /// Handle a read request from the cluster
    pub async fn handle_read_request(&self, disk_id: LocalDiskId, chunk_id: u64, offset: u32, length: u32) -> Result<Vec<u8>, String> {
        // Perform the read via chunk service
        let data = self.chunk_service.read_chunk(disk_id, chunk_id, offset, length).await?;

        println!("✅ Read {} bytes from node {}", data.len(), disk_id);
        Ok(data)
    }

    /// Handle a write request from the cluster
    pub async fn handle_write_request(&self, disk_id: LocalDiskId, chunk_id: u64, offset: u32, data: Vec<u8>) -> Result<(), String> {
        println!("📝 Write request: disk={}, chunk={}, offset={}, size={}",
                disk_id, chunk_id, offset, data.len());

        // Perform the write via chunk service
        self.chunk_service.write_chunk(disk_id, chunk_id, offset, data.clone()).await?;

        println!("✅ Wrote {} bytes to node {}", data.len(), disk_id);
        Ok(())
    }

    /// Report health status to Otto
    pub async fn report_health(&self) {
        for (disk_id, node_id) in &self.registered_nodes {
            let node_info = match self.chunk_service.get_node_info(*disk_id).await {
                Ok(info) => info,
                Err(e) => {
                    println!("❌ Failed to get node info for {}: {}", disk_id, e);
                    continue;
                }
            };

            // Update node state based on health monitoring
            // In real impl, this would check actual disk health
            let state = if node_info.used <= node_info.capacity {
                simplified_otto::NodeState::Active
            } else {
                simplified_otto::NodeState::Failed // Over-capacity = failed
            };

            self.otto.update_node_state(*node_id, state).await;
        }
    }

    /// Get cluster statistics from Otto's perspective
    pub async fn get_cluster_view(&self) -> simplified_otto::ClusterStats {
        self.otto.get_cluster_stats().await
    }

    /// Simulate periodic health reporting
    pub async fn run_health_monitor(&self) {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
            self.report_health().await;
            println!("💓 Health check completed for host {}", self.hostname);
        }
    }

    /// Get this node's contribution to load balancing
    pub async fn get_load_offers(&self) -> HashMap<LocalDiskId, u64> {
        let mut offers = HashMap::new();

        for (disk_id, node_id) in &self.registered_nodes {
            let node_info = match self.chunk_service.get_node_info(*disk_id).await {
                Ok(info) => info,
                Err(_) => continue,
            };

            if node_info.state == simplified_otto::NodeState::Active {
                let available = node_info.capacity.saturating_sub(node_info.used);
                offers.insert(*disk_id, available);
            }
        }

        offers
    }

    pub fn hostname(&self) -> &str {
        &self.hostname
    }

    pub fn registered_node_count(&self) -> usize {
        self.registered_nodes.len()
    }
}

/// Example usage and integration test
#[cfg(test)]
mod tests {
    use super::*;
    use simplified_otto::Otto;

    #[tokio::test]
    async fn test_storagenode_integration() {
        // Create Otto instance
        let otto = Arc::new(Otto::new());

        // Create StorageNode
        let mut storagenode = StorageNode::new("testhost".to_string(), otto.clone());

        // Initialize (this registers host and nodes with Otto)
        storagenode.initialize().await.expect("Failed to initialize");

        // Check that nodes were registered
        let otto_nodes = otto.get_nodes().await;
        assert_eq!(otto_nodes.len(), 3); // 3 nodes registered

        let otto_hosts = otto.get_hosts().await;
        assert_eq!(otto_hosts.len(), 1); // 1 host registered

        // Test read/write operations
        let test_data = vec![1, 2, 3, 4, 5];

        // Write data
        storagenode.handle_write_request(0 as LocalDiskId, 100, 0, test_data.clone()).await
            .expect("Write failed");

        // Read data back
        let read_data = storagenode.handle_read_request(0 as LocalDiskId, 100, 0, 5).await
            .expect("Read failed");

        assert_eq!(read_data, test_data);

        // Check cluster stats
        let stats = storagenode.get_cluster_view().await;
        assert_eq!(stats.total_hosts, 1);
        assert_eq!(stats.total_nodes, 3);
    }

    #[tokio::test]
    async fn test_node_failure_handling() {
        let otto = Arc::new(Otto::new());
        let mut storagenode = StorageNode::new("testhost".to_string(), otto.clone());

        storagenode.initialize().await.unwrap();

        // Simulate node failure by marking it over-capacity
        let node_id = storagenode.registered_nodes[&0];
        otto.update_node_usage(node_id, 2000 * 1024 * 1024 * 1024).await; // 2TB > 1TB capacity

        // Report health (should mark node as failed in Otto)
        storagenode.report_health().await;

        // Check that Otto knows about the failure
        let nodes = otto.get_nodes().await;
        assert_eq!(nodes[&node_id].state, simplified_otto::NodeState::Failed);
    }
}
