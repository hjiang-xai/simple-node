## simple-node

This is a tiny, **in-memory** Rust model of O2’s storage-node + topology-manager (“Otto”) architecture.

### Key concepts (mirrors O2 naming)

- **host**: a physical machine, in a rack
- **node**: a storage endpoint on a host (disk / device)
- **pod**: a logical grouping of nodes used to scope balancing and shard assignment
- **shard**: a slice of the keyspace (“volume” in O2 docs)
- **replica**: one copy/fragment of a shard, placed on a node

### How assignment works (simplified, but matches the shape of O2)

- **Shard → Pod**: `PodBalancer` assigns shards to pods proportional to pod capacity (sum of node weights).
- **Replica → Node**: replicas of each shard are assigned to nodes *within the shard’s pod*, preferring **rack diversity**.
- **Register/Deregister node**:
  - register: node appears in roster → node balancer puts it into a pod (or keeps it as spare) → shard/replica placement may shift
  - deregister (unload): node becomes unassignable → replicas on it are considered free → replica balancer reassigns them

### Run

```bash
cargo run
```


