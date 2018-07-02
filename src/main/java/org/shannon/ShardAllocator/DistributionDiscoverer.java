package org.shannon.ShardAllocator;

import java.util.Collection;
import java.util.Map;

/**
 * That which discovers the distribution of control of shards.
 *
 * @param <Node>    That which controls or has ownership of Shards
 * @param <Shard>   Some fragment of the whole which needs controlling.
 */
@FunctionalInterface
public interface DistributionDiscoverer<Node, Shard> {
  /**
   * That which discovers the distribution of control of shards.
   *
   * @return    A mapping of which nodes report control of which shards.
   */
  Map<Node, Collection<Shard>> discoverDistribution();
}
