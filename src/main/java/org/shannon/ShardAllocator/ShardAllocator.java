package org.shannon.ShardAllocator;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;

public interface ShardAllocator<Node, Shard> extends Closeable{
  public void notifyShardsChange(Collection<Shard> shardUniverse);
  public void notifyNodesChange(Collection<Node> nodeUniverse);
  public void notifyDistributionChange(Map<Node, Collection<Shard>> distribution);
  public void awaitRebalance();
}
