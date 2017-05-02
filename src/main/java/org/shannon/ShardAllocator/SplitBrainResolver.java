package org.shannon.ShardAllocator;

import java.util.Collection;
import java.util.HashSet;

import com.google.common.collect.TreeMultimap;

public interface SplitBrainResolver<Node, Shard> {
  public Collection<ShardRelocation<Node, Shard>> resolve(Shard shard, HashSet<Node> nodes
      , TreeMultimap<Integer, Node> nodesByCount);
}
