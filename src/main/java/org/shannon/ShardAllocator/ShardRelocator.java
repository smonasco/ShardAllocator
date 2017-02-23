package org.shannon.ShardAllocator;

@FunctionalInterface
public interface ShardRelocator<Node, Shard> {
  public void relocate(ShardRelocation<Node, Shard> relocation);
}
