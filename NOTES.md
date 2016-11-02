Toplogy changes are propagated via the same mechanism as normal replication. This requires that we have have some way of segregating client data replications from toplogy changes.

So, we have the same CAS style operation using the index offset.

```rust
pub enum ReplicaRequest {
    AppendLogEntry {
        assumed_offset: LogPos,
        entry_offset: LogPos,
        datum: LogContent,
    },
}

pub enum LogContent {
  ClientDatum(Vec<u8>),
  ToplogyChanged(TopologyConfig),
}

pub struct TopologyConfig {
  epoch: Epoch,
  members: Map<Index, MemberMeta>,
}

// Wherin the meaningful state for each member is:

pub struct MemberView {
  epoch: Epoch,
  downstream: Option<MemberMeta>
}
```

However, the log is essentially a sorted key-value map. So, it doesn't seem unreasonable to encode the type into the key, eg: `(Epoch?, LogPos, LogContent.discriminator)`, unique over the (Epoch,LogPos).

So, we end up with a task that tails the log for toplogy configs, maps them to a memberview, and then tries replicating to any current downstream. Because of the bad sequence response, you can just try starting from zero, and it'll tell you when you're wrong.

# View changes

Switch from topology change "events" to a configured "State". Initialize each
replica with a default containing self, then over-write?

Move to a "lease" based mechanism for keeping replicas alive. Each replica
in a chain connects to the chain's configuration manager, and periodically
sends a ping to demonstrate aliveness. Once the CM detects a toplogy change,
it sends that change to the (new?) head.

The configuration manager could be a system like etcd/zk, or a preceeding
chain manager.

So, there are two interfaces, one that controls the chain head, and a
heartbeating mechanism for each member.

From S4.7 of the "Leveraging" paper:

> Replicas in each shard monitor the replicas in the shard they are
> sequencing. When replica failure is suspected, the wedge command is issued
> to the victim shard by both the suspecting replicas in the same shard and
> the sequencer shard. This is done to prevent the shard from accepting
> any new client requests and guarantee that safety is not violated. Next,
> the sequencer issues a reconfiguration of the wedged shard and spins
> up new replicas if needed. Figure 13 highlights the fact that because
> elastic Replication guarantees safety independent of timeout values,
> aggressively small timeout values can used for failure detection without
> compromising strong consistency.

With an active CM line in Elastic Replication, you can just have the CM send a
re-configuration message. With a "passive" mechanism (eg: etcd) you'll need an
intermediary to mediate. That could however live alongside the replica. 

Currently(ish), the re-configuration interface requires a CAS from the old config
to the new. So, we'll need two "tasks". 1 for heart-beating.

--
"Leveraging": Leveraging Sharding in the Design of Scalable Replication Protocols
