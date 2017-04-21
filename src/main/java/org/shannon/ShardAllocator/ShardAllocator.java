package org.shannon.ShardAllocator;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public interface ShardAllocator<Node, Shard> extends Closeable{
  public void notifyShardsChange(HashSet<Shard> shardUniverse);
  public void notifyNodesChange(HashSet<Node> nodeUniverse);
  public void notifyDistributionChaing(Map<Node, Collection<Shard>> distribution);
}
