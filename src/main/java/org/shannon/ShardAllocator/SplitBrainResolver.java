package org.shannon.ShardAllocator;

import java.util.Collection;
import java.util.HashSet;

import com.google.common.collect.TreeMultimap;

/**
 * When more than 1 node is reported to own the same shard this will be called with the
 * shard in question and the nodes that reported owning the shard.
 *
 * It should return the appropriate steps for resolving the issue.
 *
 * @param <Node>    That which controls or has ownership of Shards
 * @param <Shard>   Some fragment of the whole which needs controlling.
 */
public interface SplitBrainResolver<Node, Shard> {
  public Collection<ShardRelocation<Node, Shard>> resolve(Shard shard, HashSet<Node> nodes
      , TreeMultimap<Integer, Node> nodesByCount);
}
