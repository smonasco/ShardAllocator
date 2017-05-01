package org.shannon.ShardAllocator.mock;

import static org.junit.Assert.assertEquals;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.shannon.ShardAllocator.ShardRelocation;
import org.shannon.ShardAllocator.Impl.SimpleAllocator;

public class SimpleAllocatorWrapper implements Closeable {
  private final SimpleAllocator<Integer, Integer> allocator;
  public HashSetValuedHashMap<Integer, Integer> dist;
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
            relocate(relocation);
          }
        }, 1      
      );
  }
  
  private void relocate(ShardRelocation<Integer, Integer> relocation) {
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

  public void notifyNodeChange(Collection<Integer> nodes) {
    this.nodes = nodes;
    dist.keySet().retainAll(nodes);
    allocator.notifyNodesChange(nodes);    
  }
  
  public void notifyDistributionChange(HashSetValuedHashMap<Integer, Integer> dist) {
    this.dist = dist;
    allocator.notifyDistributionChange(dist.asMap());
  }

  public void awaitRebalance() {
    allocator.awaitRebalance();
  }
  
  public void isBalanced() {
    try {
      double mean = (double)shards.size() / (double) nodes.size();
      int fmean = (int) Math.floor(mean);
      int cmean = (int) Math.ceil(mean);
      
      assertEquals("Should have allocations for every node", Math.min(nodes.size(), shards.size()), dist.keySet().size());
      int cmeanCount = 0;
      int fmeanCount = 0;
      for(Collection<Integer> shards : dist.asMap().values()) {
        if (shards.size() == fmean) { //if cmean == fmean then we end up in this bucket
          ++fmeanCount; 
        } else {
          assertEquals("Can only be ceiling or floor of the mean shards per node", cmean, shards.size());
          ++cmeanCount;
        }
      }
      assertEquals("Should have remainder count of over allocated", shards.size() % nodes.size(), cmeanCount);
      assertEquals("All others should have the floor of the mean", nodes.size() - cmeanCount, fmeanCount);
    } catch(Throwable e) {
      System.out.println(e);
      throw e;
    }
  }
}
