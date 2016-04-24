# Refactoring:
## Tease out replication model + log into a seperate thread from the I/O handlers.
Has current points of coupling:

 * When the Upstream connects to a downstream replica, the downstream needs to inform the upstream of it's water marks. This needs the current sequence no, &c.

Seems like we should be able to tease out a modicum of routing; at the moment, the replica knows about per-connection tokens, because the replica is responsible for knowing which operations are currently in-flight. This was intended to avoid disconnecting clients who may have outstanding requests.  We can factor this out into some "client proxy" logic, which handles tracking outstanding client requests, but otherwise forwards everything onto the actual head replica. Cf. Client proxy in the ChainReaction paper.  So:

 * Move the client request tracking logic into a ClientProxy object
 * Replace use of `mio::Token` in the `Replica` with a logical description of it's relationship, eg: `Upstream`, `Downstream`. Or at least...

Express interfaces in terms of messages; tell-don't-ask style. So:

 * Define a trait for Upstream/downstream messages
 * Replication session ID consists of (address ref@epoch). Logically, the replication process begins anew for each configuration epoch.
 * So, because replication messages are only valid for a particular configuration, we can assume that all peers logically reconnect for each epoch.
* Also, probably best to have upstream ask downstream peer for water marks, rather than relying on implicit banner on connect. This means we do not need to retain any state across configurations, and more consistency in message patterns.
 
## Configuration mechanism is weird

A few models of Chain Replication (eg: CORFU) talk about rendering a replica immutable, where the optimisation would be to inherit an existing replica's data. At the moment, we get a struct from the coordinator (ie: the threads that poke `etcd`), which we then interpret on an ad-hoc basis, prodding the `Replica` as we go. It might be better to dispose of the existing replica object, and re-build it, factory-style, from the configuration.