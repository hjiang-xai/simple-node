use simple_node::model::{ClusterConfig, NodeState, ShardId};
use simple_node::otto::Otto;
use simple_node::storage_node::StorageNodeAgent;

fn print_mapping(otto: &Otto) {
    println!("\n=== Pods ===");
    for (pod_id, pod) in &otto.mapping.pods {
        let mut nodes: Vec<_> = pod.nodes.iter().copied().collect();
        nodes.sort_by_key(|n| n.0);
        let mut shards: Vec<_> = pod.shards.iter().copied().collect();
        shards.sort_by_key(|s| s.0);
        println!(
            "pod {:?}: nodes={:?} shards={} (e.g. {:?})",
            pod_id,
            nodes,
            shards.len(),
            shards.get(0)
        );
    }

    let mut spares: Vec<_> = otto.mapping.spares.iter().copied().collect();
    spares.sort_by_key(|n| n.0);
    println!("spares={spares:?}");

    // Show one shard's replica placement as an example of shard->pod->node chain.
    let example_shard = ShardId(0);
    println!("\n=== Example shard placement ===");
    let pod = otto.mapping.shard_to_pod.get(&example_shard);
    println!("shard {:?} -> pod {:?}", example_shard, pod);
    for (replica_id, node_id) in otto.replica_assignments_for_shard(example_shard) {
        println!("  replica {:?} -> node {:?}", replica_id, node_id);
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let cfg = ClusterConfig {
        num_shards: 16,
        replica_factor: 2,
        target_pod_width: 3,
        min_spare_nodes: 1,
    };
    let mut otto = Otto::new(cfg);

    // Register a few "hosts" each with multiple "nodes" (disks).
    let mut h1 = StorageNodeAgent::new("host-a", "rack-1");
    h1.initialize(
        &mut otto,
        vec![
            (0, "sda".to_string(), 100),
            (1, "sdb".to_string(), 100),
            (2, "sdc".to_string(), 100),
        ],
    )?;

    let mut h2 = StorageNodeAgent::new("host-b", "rack-2");
    h2.initialize(
        &mut otto,
        vec![
            (0, "sda".to_string(), 100),
            (1, "sdb".to_string(), 100),
            (2, "sdc".to_string(), 100),
        ],
    )?;

    println!("Initial rebalance...");
    let r = otto.rebalance();
    println!("moved_shards={} moved_replicas={}", r.moved_shards, r.moved_replicas);
    print_mapping(&otto);

    // Register a new node (like a new disk added) and rebalance.
    println!("\nRegistering a new node on host-b...");
    let host_b_id = h2.host_id.expect("initialized");
    let new_node = otto.register_node(host_b_id, "nvme0n1".to_string(), 200)?;
    let r = otto.rebalance();
    println!("moved_shards={} moved_replicas={}", r.moved_shards, r.moved_replicas);
    print_mapping(&otto);

    // Deregister (unload) a node and rebalance.
    println!("\nUnloading node {:?} ...", new_node);
    otto.mark_node_state(new_node, NodeState::Unload)?;
    let r = otto.rebalance();
    println!("moved_shards={} moved_replicas={}", r.moved_shards, r.moved_replicas);
    print_mapping(&otto);

    Ok(())
}


