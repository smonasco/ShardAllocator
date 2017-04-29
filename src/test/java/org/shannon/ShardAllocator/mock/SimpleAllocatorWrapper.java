package org.shannon.ShardAllocator.mock;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.shannon.ShardAllocator.ShardRelocation;
import org.shannon.ShardAllocator.Impl.SimpleAllocator;

public class SimpleAllocatorWrapper implements Closeable {
  private final SimpleAllocator<Integer, Integer> allocator;
  public final HashSetValuedHashMap<Integer, Integer> dist;
  public final AtomicInteger moveCount = new AtomicInteger(0);
  public final AtomicInteger discoveryCount = new AtomicInteger(0);
  public Collection<Integer> nodes;
  public Collection<Integer> shards;
  private final Object sync = new Object();
  
  public SimpleAllocatorWrapper(Collection<Integer> nodes, Collection<Integer> shards
      , HashSetValuedHashMap<Integer, Integer> dist) {
    this.dist = dist;
    this.nodes = nodes;
    this.shards = shards;
    allocator = new SimpleAllocator<Integer, Integer>(
        nodes,
        shards,
        dist.asMap(),
        () -> { 
          discoveryCount.incrementAndGet();
          return dist.asMap();
        }, (relocation) -> {
          synchronized(sync) {
            relocate(dist, relocation);
          }
        }, 1      
      );
  }
  
  private void relocate(HashSetValuedHashMap<Integer, Integer> dist, ShardRelocation<Integer, Integer> relocation) {
    moveCount.incrementAndGet();
    if (relocation.getFromNode() != null) { dist.removeMapping(relocation.getFromNode(), relocation.getShard()); }
    if (relocation.getToNode() != null) { dist.put(relocation.getToNode(), relocation.getShard()); }
  }

  public void notifyShardChange(Collection<Integer> shards) {
    this.shards = shards;
    allocator.notifyShardsChange(shards); 
  }

  public void close() {
    allocator.close();    
  }
  
}
