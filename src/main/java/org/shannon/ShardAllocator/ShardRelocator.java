package org.shannon.ShardAllocator;

/**
 * That which understands how to relocate shards.
 *
 * @param <Node>    That which controls or has ownership of Shards
 * @param <Shard>   Some fragment of the whole which needs controlling.
 */
@FunctionalInterface
public interface ShardRelocator<Node, Shard> {
  public void relocate(ShardRelocation<Node, Shard> relocation);
}
