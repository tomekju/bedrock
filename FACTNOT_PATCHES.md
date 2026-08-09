# Factnot Bedrock Recovery Fork

This branch carries Factnot's reviewed recovery and durability changes on top
of the upstream Bedrock `0.5.0` release.

## Provenance

- Upstream repository: <https://github.com/bedrock-kv/bedrock>
- Upstream release tag: `0.5.0`
- Upstream base commit: `49fc51ffe3ff3e76cd9f21510904af199ca315d7`
- Patch source: Factnot Fuu commit
  `e40b0c68b11cebcaacd0d15232bcec82572dae55`
- Original license: MIT; the upstream `LICENSE` file is retained unchanged.

## Scope

The patch set hardens fresh-cluster admission, recovery topology, log and
materializer recruitment, snapshotless bootstrap, durable checkpointing,
materializer catch-up, shard metadata, waiter wake-up, and recovered layout
normalization. It is intentionally pinned by Fuu to an immutable commit.

This fork does not by itself establish production readiness. Factnot's
three-node recovery, object-storage, restore, privacy, and release gates remain
authoritative.
