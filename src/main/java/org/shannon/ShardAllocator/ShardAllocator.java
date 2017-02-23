package org.shannon.ShardAllocator;

import java.io.Closeable;
import java.util.HashSet;

public interface ShardAllocator<Node, Shard> extends Closeable{
  public void notifyShardsChange(HashSet<Shard> shardUniverse);
  public void notifyNodesChange(HashSet<Node> nodeUniverse);
}
