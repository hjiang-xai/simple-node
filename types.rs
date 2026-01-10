//! Shared types for simplified Otto components

/// Unique identifier for nodes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);

/// Unique identifier for hosts
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HostId(pub u64);

/// Node states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Active,
    Inactive,
    Failed,
}

/// Represents a storage node (disk) in the cluster
#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub host_id: HostId,     // Which host this node belongs to
    pub device_name: String, // e.g., "sda", "nvme0n1"
    pub capacity: u64,       // Total storage capacity
    pub used: u64,           // Currently used capacity
    pub state: NodeState,
}

impl Node {
    pub fn new(id: NodeId, host_id: HostId, device_name: String, capacity: u64) -> Self {
        Self {
            id,
            host_id,
            device_name,
            capacity,
            used: 0,
            state: NodeState::Active,
        }
    }

    pub fn available_capacity(&self) -> u64 {
        self.capacity.saturating_sub(self.used)
    }

    pub fn utilization_percent(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            (self.used as f64 / self.capacity as f64) * 100.0
        }
    }
}
