# Refactoring:
## Tease out replication model + log into a seperate thread from the I/O handlers.
Has current points of coupling:

 * When the Upstream connects to a downstream replica, the downstream needs to inform the upstream of it's water marks. This needs the current sequence no, &c.

Seems like we should be able to tease out a modicum of routing; at the moment, the replica knows about per-connection tokens, because the replica is responsible for knowing which operations are currently in-flight. This was intended to avoid disconnecting clients who may have outstanding requests.  We can factor this out into some "client proxy" logic, which handles tracking outstanding client requests, but otherwise forwards everything onto the actual head replica. Cf. Client proxy in the ChainReaction paper.  So:

 * Move the client request tracking logic into a ClientProxy object
 * Replace use of `mio::Token` in the `Replica` with a logical description of it's relationship, eg: `Upstream`, `Downstream`. Or at least...

 * The replica has to process client inputs differently from replica messages because each message has a sequence number attached; and currently, the replica knows that.
 * We could have an "upstream" port(slot) that is either a ClientProxy, or an upstream peer connection.
 * Similarly, we could shift ReplRole logic out of the replica, and consider the downstream as either a downstream peer or a terminating app model.

Express interfaces in terms of messages; tell-don't-ask style. So:

 * Define a trait for Upstream/downstream messages
 * Replication session ID consists of (address ref@epoch). Logically, the replication process begins anew for each configuration epoch.
 * So, because replication messages are only valid for a particular configuration, we can assume that all peers logically reconnect for each epoch.
* Also, probably best to have upstream ask downstream peer for water marks, rather than relying on implicit banner on connect. This means we do not need to retain any state across configurations, and more consistency in message patterns.
 

## Tracing

Use `hybrid-clocks` implementation to stamp each network event with a session + logical timestamp.

## Anti-Entropy
Frankly, I'd not trust my code. So, it'd be great to have a mechanism to monitor chain integrity for each shard. at least:

 * Merkle tree verifier for the log at some given logical time; best if trivially diffable listing (eg: sorted Json tree, or just a csv listing of (level, range, hash)
 * To get usefully comparable trees, we need to essentially snapshot the log; handily, this would just be taking some clock value `C` from the tail and reading log entries before `C`.
 * To start with we can expose this via HTTP or similar; but eventually using a gossip mechanism to exchange this would be useful.

The "obvious" thing to do is have as many levels are there are bits in the sequence identifier; which leads to a fanout of 2^1 at each level. But that then leads to `O(n*log2(n))` nodes in the tree; and for a 64-bit sequence identifier; 64 levels (unless we do something cunning with exponential trees, or just stop when the effectively measured fanout is 1).

We can decrease the number of levels (and complexity) by picking the Mth-bit from each identifier, resulting in a fanout of 2^M at each level, and `O(n*log_2^M(n))` or `O(n*log2(n)/M)`.


### Merkle tree log validation

Use a Riak-style merkle tree to allow validation of log-stores between peers. Assuming that we stick to using a fixed-length key in the DB (ie: sequence number for a record ), then it becomes comparatively easy to build a 2^N-ary trie, as we need to keep track of the longest-observed length in order to determine how far from the leaves the root should be. Whilst most systems (eg: Riak) use a hash function to derive a fixed-length key (and so path in the trie), we know that we only ever append to the log, and that updates will be comparatively dense, so we can minimise the amount of churn in the tree by batching updates. This also makes it easier to identify which portion of the log is wrong, as each branch then identifies a contiguous range.

In order to create a useful summary, we need to identify a causal cut across a partition (which is easy as we are using chain replication), and calculate the hash-tree across that partition.



## Configuration mechanism is weird

A few models of Chain Replication (eg: CORFU) talk about rendering a replica immutable, where the optimisation would be to inherit an existing replica's data. At the moment, we get a struct from the coordinator (ie: the threads that poke `etcd`), which we then interpret on an ad-hoc basis, prodding the `Replica` as we go. It might be better to dispose of the existing replica object, and re-build it, factory-style, from the configuration.
