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

## Integration

We assume that a "client-proxy" module is co-located with each replica
instance, which forwards writes/reads to the head/tail of the chain
respectively.

The client-proxy is (currently) the point of entry for view-changes as well as
client-writes, as it provides a relatively convenient method of approximately
sequencing requests. We'll need to include an epoch in the replication
protocol to avoid a client-proxy that hasn't yet observed a view change
sending writes to what used to be the head. That said, we aim to ensure that
new nodes can only be added to the tail.

The interface for an active participant would be:

```
interface ViewChanges {
  viewChanged(from: (Epoch, Seqno), to: (Epoch, Seqno), view: ChainView)
}
```

When the interface from the intermediary to the view manager would be:
```
interface ReplicaHeatbeats {
  iAmAlive(node: HostConfig)
}
```

Or similar.

So assuming we have a failure detection mechanism that provides a serial set
of configurations, eg: a set of nodes with time-to-live in etcd, we can watch
for changes in the store, and assemble those into views.

Then, when a node's view proxy observes a configuration in which it is
the head, it'll "adopt" it, and send the ViewChange message down the chain.

This does however assume that the view-proxy / heartbeater and replicator all
exist in the same failure domain. Assuming that the view-proxy and replicator
both participate in the epoch mechanism, we *should* be safe, but I'd like
more confidence than just a hunch.

```
views:
  heartbeater:
    Sender<Heartbeat>,
  viewproxy:
    Receiver<ChainConfig>,
    Sender<ChainConfig>,
```

So, the heartbeater would be a widget that connects to the etcd, reserves a
slot and returns a `Sender<Heartbeat>` or `Service<Request=(), Response=()>`.

The view proxy then becomes a function of a
`Stream<Item=(CreateTimestamp, Option<HostConfig>)>` ->
`Stream<Item=ChainView>`.

# References
"Leveraging": Leveraging Sharding in the Design of Scalable Replication Protocols
