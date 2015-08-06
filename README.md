# erloom

With a *loom* you can have your CAP and eat it too.
Each operation can have its own semantics w.r.t. replication and consistency.
At its core, a loom is just a replicated set of logs, one for each participating node.
Looms take care of synchronization automagically, making it trivial to share state.