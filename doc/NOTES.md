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

From §4.7 of the "Leveraging" paper:

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
to the new. So, we'll need two "tasks". one for heart-beating, another for
monitoring the view-manager.

## Splitting concerns

Alternatively, we do not actually need the failure detection and configuration
sequencing to live in the same place. If we allow the replication protocol to
include empty heartbeat messages, then we can have each peer in the chain
monitor it's upstream and downstream, and report suspected failures to the
config sequencer.

This might well give us faster detection over a pure heartbeat-to-view-manager
mechanism, as peers will be exchanging more messages, and so it should become
more readily apparent if a peer is suspected. If a downstream is looking like
it may be suspected (eg: 90% confidence of failure with Φ accural), the
replica can send a probe.

However, because connections to the sequencer may fail, we'll need to rely on
redundancy in the sequencer; this might end up being our client-proxy.

However, in order to advertise it's membership, each node will need to contact
the view-manager anyway, so it makes sense to start off with the pure
heartbeat mechanism, and add inter-peer failure detection as an optimisation
later.

## Wedging

So, rather than relying on an external failure detector, we could have each
peer observe it's downstream / upstream; and wedge the replica state iff any
peer suspects a failure of an upstream/downstream. next, we need to propagate.

Assuming crash-stop failures, or equivalent, when a node fails, both it's
upstream and downstream should employ a failure detector for a peer, by:

 * The upstream sends periodical heartbeat messages, and logs responses to FD.
 * The downstream will await heartbeats, and log them to it's FD.

If we assume that the replica state machine can get notified when the failure
detector suspects a failure, then we can:

  * Cancel/fail outstanding RPCs to that machine
  * Record view change marking the current view as wedged (so all messages
    with that same epoch fail as wedged).
  * Ensure that the "wedge" view change command does get replicated downstream.
  * Reject any further view-changes for that view

However, we'll then need an external view manager to select a new view and
push the config to the new nodes.

However, all we need there (as mentioned elsewhere) is sequencing, so we could
just do that with a CAS to an external sequencer (eg: etcd, postgres) and
stamp configuration changes with the sequencer's version number.

## Integration with passive CM (etcd, etc)

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

# View manager glue

Okay, so our interface from the external view manager to the intermediary
(client-proxy or thick client), would pretty much look like this:

```
interface ViewChanges {
  viewChanged(from: Epoch, to: Epoch, view: ChainView)
}
```

Which is essentially a CAS from the previous view to the new view, the new
view supplanting the old.

Now, the fun part here is how the CAS of epochs relates to the CAS in terms of
Sequence numbers. In order to prevent a head that has been presumed dead and
removed from a new configuration writing data to the new head, we'll need to
nick Raft's Epoch mechanism along with the log sequence numbers. So, we'll go
from the AppendLogEntry request being:

```
AppendLogEntry(assumed_offset: LogPos, entry_offset: LogPos, datum: LogEntry)
```

Where the LogPos is defined as the sequence number, we'll replace the offsets
with tuples of `(Epoch, LogPos)`, with essentially the same CAS semantics.

So, the viewChanged method would end up being something like:
```
  fn viewChanged(&mut self, assumed_epoch:Epoch, next_epoch:Epoch, view:ChainView) -> Result<()> {
    if assumed_epoch != self.current_epoch || next_epoch <= self.current_epoch {
      return Err(ErrorKind::WrongEpoch(self.current_epoch))
    };
    let entry = LogEntry::ViewChanges(view);
    loop {
      let current = (self.current_epoch, self.last_sent_pos);
      let next = (next_epoch, self.last_sent_pos.next());
      self.current_epoch = next_epoch;
      self.last_sent_pos = self.last_sent_pos.next();
      if self.next.append_log_entry(current, next, entry) {
        return Ok(())
      } else {
        // If BadSequence response has a different Epoch from `assumed_epoch`, then
        // fail the request.
        // If BadSequence response has the same Epoch, then reset
        // `last_sent_pos` and resend.
      }
    }
  }
```

# References
"Leveraging": Leveraging Sharding in the Design of Scalable Replication Protocols
