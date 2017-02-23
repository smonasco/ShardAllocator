package org.shannon.ShardAllocator;

import java.util.Collection;
import java.util.Map;

@FunctionalInterface
public interface DistributionDiscoverer<Node, Shard> {
  public Map<Node, Collection<Shard>> discoverDistribution();
}
