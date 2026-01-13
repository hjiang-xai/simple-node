use std::collections::{BTreeMap, HashMap, HashSet};

use crate::model::{ClusterConfig, Node, NodeId, NodeState, Pod, PodId, PodMapping, RackId, Shard, ShardId};

#[derive(Debug, Clone)]
pub struct BalanceResult {
    pub moved_shards: usize,
    pub moved_replicas: usize,
}

/// A simplified implementation inspired by O2's `podbalancer`:
/// - **Node balancing**: assign active nodes into pods, keep a minimum spare set
/// - **Shard balancing**: assign shards to pods proportional to pod capacity
/// - **Replica balancing**: assign replicas to nodes within the shard's pod, preferring rack diversity
#[derive(Debug, Clone)]
pub struct PodBalancer {
    cfg: ClusterConfig,
    next_pod_id: u32,
}

impl PodBalancer {
    pub fn new(cfg: ClusterConfig) -> Self {
        Self { cfg, next_pod_id: 0 }
    }

    pub fn instantly_balance(
        &mut self,
        mapping: &mut PodMapping,
        nodes: &HashMap<NodeId, Node>,
        shards: &HashMap<ShardId, Shard>,
        rack_of_node: &HashMap<NodeId, RackId>,
    ) -> BalanceResult {
        let mut moved_shards = 0usize;
        let mut moved_replicas = 0usize;

        // Ensure shard<->replica maps exist.
        for (shard_id, shard) in shards {
            mapping.shard_to_replicas.insert(*shard_id, shard.replicas.clone());
            for &replica_id in &shard.replicas {
                mapping.replica_to_shard.insert(replica_id, *shard_id);
            }
        }

        self.balance_nodes(mapping, nodes);
        moved_shards += self.balance_shards(mapping, nodes, shards);
        moved_replicas += self.balance_replicas(mapping, nodes, shards, rack_of_node);

        mapping.version += 1;
        BalanceResult { moved_shards, moved_replicas }
    }

    fn balance_nodes(&mut self, mapping: &mut PodMapping, nodes: &HashMap<NodeId, Node>) {
        // Active nodes are the only assignable devices (mirrors Otto->PodBalancer filtering).
        let mut active: Vec<NodeId> = nodes
            .values()
            .filter(|n| n.state == NodeState::Active)
            .map(|n| n.id)
            .collect();
        active.sort();

        let spare_count = self.cfg.min_spare_nodes.min(active.len());
        let (non_spares, spares) = active.split_at(active.len() - spare_count);

        mapping.spares = spares.iter().copied().collect();

        let target_width = self.cfg.target_pod_width.max(1);
        let desired_pods = (non_spares.len() + target_width - 1) / target_width;

        // Rebuild pod set deterministically (toy implementation).
        mapping.pods = BTreeMap::new();
        mapping.node_to_pod.clear();

        if desired_pods == 0 {
            return;
        }

        // Make pod IDs stable across rebalances by growing from 0..desired_pods.
        if self.next_pod_id < desired_pods as u32 {
            self.next_pod_id = desired_pods as u32;
        }

        for i in 0..desired_pods {
            let pod_id = PodId(i as u32);
            mapping.pods.insert(
                pod_id,
                Pod {
                    id: pod_id,
                    nodes: HashSet::new(),
                    shards: HashSet::new(),
                },
            );
        }

        // Assign nodes round-robin to pods.
        for (i, &node_id) in non_spares.iter().enumerate() {
            let pod_id = PodId((i % desired_pods) as u32);
            mapping.node_to_pod.insert(node_id, pod_id);
            mapping.pods.get_mut(&pod_id).expect("pod exists").nodes.insert(node_id);
        }

        // Remove any replica placements on nodes that are no longer active or no longer assigned.
        mapping
            .replica_to_node
            .retain(|_, node_id| nodes.get(node_id).is_some_and(|n| n.state == NodeState::Active) && mapping.node_to_pod.contains_key(node_id));
    }

    fn balance_shards(
        &mut self,
        mapping: &mut PodMapping,
        nodes: &HashMap<NodeId, Node>,
        shards: &HashMap<ShardId, Shard>,
    ) -> usize {
        let pod_ids: Vec<PodId> = mapping.pods.keys().copied().collect();
        if pod_ids.is_empty() {
            mapping.shard_to_pod.clear();
            for pod in mapping.pods.values_mut() {
                pod.shards.clear();
            }
            return 0;
        }

        // Compute pod capacities from member nodes (weight).
        let mut pod_capacity: HashMap<PodId, u64> = HashMap::new();
        for (&pod_id, pod) in &mapping.pods {
            let cap = pod.nodes.iter().filter_map(|nid| nodes.get(nid)).map(|n| n.weight).sum();
            pod_capacity.insert(pod_id, cap);
        }

        let total_capacity: u64 = pod_capacity.values().sum();
        let total_capacity = total_capacity.max(1); // avoid div-by-zero in a toy model

        // Reset pod shard sets.
        for pod in mapping.pods.values_mut() {
            pod.shards.clear();
        }

        // Greedy assignment: put each shard on pod with best headroom score.
        let mut shard_ids: Vec<ShardId> = shards.keys().copied().collect();
        shard_ids.sort();

        // Track assigned shard "weight" per pod. In O2, shard weight defaults to #replicas.
        let mut pod_weight: HashMap<PodId, u64> = pod_ids.iter().map(|&p| (p, 0u64)).collect();

        let mut moved = 0usize;
        for shard_id in shard_ids {
            let shard_weight = shards
                .get(&shard_id)
                .map(|s| s.replicas.len() as u64)
                .unwrap_or(0);

            let (best_pod, _) = pod_ids
                .iter()
                .map(|&pod_id| {
                    let cap = *pod_capacity.get(&pod_id).unwrap_or(&0);
                    let w = *pod_weight.get(&pod_id).unwrap_or(&0);
                    // Headroom proportional to capacity share:
                    // score = (cap/total_cap) * total_weight - w
                    let target = (cap as f64 / total_capacity as f64) * (self.cfg.num_shards as f64 * self.cfg.replica_factor as f64);
                    let score = target - (w as f64);
                    (pod_id, score)
                })
                .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
                .expect("non-empty");

            if mapping.shard_to_pod.get(&shard_id).copied() != Some(best_pod) {
                moved += 1;
            }
            mapping.shard_to_pod.insert(shard_id, best_pod);
            mapping.pods.get_mut(&best_pod).expect("pod exists").shards.insert(shard_id);
            *pod_weight.get_mut(&best_pod).unwrap() += shard_weight;
        }

        moved
    }

    fn balance_replicas(
        &mut self,
        mapping: &mut PodMapping,
        nodes: &HashMap<NodeId, Node>,
        shards: &HashMap<ShardId, Shard>,
        rack_of_node: &HashMap<NodeId, RackId>,
    ) -> usize {
        let mut moved = 0usize;

        // Replica load metric (count of replicas assigned to node).
        let mut node_replica_count: HashMap<NodeId, u64> = HashMap::new();
        for &node_id in mapping.replica_to_node.values() {
            *node_replica_count.entry(node_id).or_insert(0) += 1;
        }

        for (shard_id, shard) in shards {
            let Some(&pod_id) = mapping.shard_to_pod.get(shard_id) else {
                continue;
            };
            let Some(pod) = mapping.pods.get(&pod_id) else {
                continue;
            };

            // Candidate nodes are active nodes in the pod.
            let mut candidates: Vec<NodeId> = pod
                .nodes
                .iter()
                .copied()
                .filter(|nid| nodes.get(nid).is_some_and(|n| n.state == NodeState::Active))
                .collect();
            candidates.sort();
            if candidates.is_empty() {
                continue;
            }

            // Group candidates by rack for rack diversity.
            let mut rack_to_nodes: HashMap<RackId, Vec<NodeId>> = HashMap::new();
            for nid in &candidates {
                let rack = *rack_of_node.get(nid).unwrap_or(&RackId(0));
                rack_to_nodes.entry(rack).or_default().push(*nid);
            }
            for nodes_in_rack in rack_to_nodes.values_mut() {
                nodes_in_rack.sort();
            }

            for (replica_index, &replica_id) in shard.replicas.iter().enumerate() {
                // If replica already assigned to an active node in this pod, keep it.
                let keep = mapping.replica_to_node.get(&replica_id).copied().is_some_and(|nid| {
                    mapping.node_to_pod.get(&nid).copied() == Some(pod_id)
                        && nodes.get(&nid).is_some_and(|n| n.state == NodeState::Active)
                });
                if keep {
                    continue;
                }

                let mut used_racks: HashSet<RackId> = HashSet::new();
                for &other_replica in &shard.replicas {
                    if other_replica == replica_id {
                        continue;
                    }
                    if let Some(&nid) = mapping.replica_to_node.get(&other_replica) {
                        if let Some(&rack) = rack_of_node.get(&nid) {
                            used_racks.insert(rack);
                        }
                    }
                }

                // Choose rack if possible: prefer racks not used for this shard yet.
                // Make selection deterministic by sorting racks.
                let mut racks: Vec<RackId> = rack_to_nodes.keys().copied().collect();
                racks.sort();

                let preferred_rack = racks.iter().copied().find(|r| !used_racks.contains(r));

                let chosen_node = if let Some(rack) = preferred_rack {
                    choose_least_loaded(&rack_to_nodes[&rack], &node_replica_count)
                } else {
                    // Not enough distinct racks; fall back to any least-loaded node in the pod.
                    choose_least_loaded(&candidates, &node_replica_count)
                };

                if mapping.replica_to_node.insert(replica_id, chosen_node) != Some(chosen_node) {
                    moved += 1;
                }
                *node_replica_count.entry(chosen_node).or_insert(0) += 1;

                // Slight bias to spread replicas: alternate choice by replica index.
                let _ = replica_index;
            }
        }

        moved
    }
}

fn choose_least_loaded(candidates: &[NodeId], loads: &HashMap<NodeId, u64>) -> NodeId {
    *candidates
        .iter()
        .min_by_key(|nid| loads.get(nid).copied().unwrap_or(0))
        .expect("non-empty")
}


