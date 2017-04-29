package org.shannon.ShardAllocator.test;

import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.junit.Test;
import org.shannon.ShardAllocator.Impl.SimpleAllocator;
import org.shannon.ShardAllocator.mock.SimpleAllocatorWrapper;
import org.shannon.util.TestClass;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class SimpleAllocatorTest extends TestClass {
  
  private static HashSet<Integer> integers(int from, int to) {
    HashSet<Integer> retval = new HashSet<Integer>();
    for(int i = from; i <= to; ++i) {
      retval.add(i);
    }
    return retval;
  }
  
  private HashSetValuedHashMap<Integer, Integer> dist(Integer... a) {
    HashSetValuedHashMap<Integer, Integer> dist = new HashSetValuedHashMap<Integer, Integer>();
    int nextShard = 0;
    int curNode = 0;
    for (Integer i : Arrays.asList(a)) {
      dist.putAll(curNode++, integers(nextShard, nextShard + i - 1));
      nextShard += i;
    }
    return dist;
  }
  
  private HashSetValuedHashMap<Integer, Integer> balancedDist(int nodeCount, int loadCount) {
    Integer[] counts = new Integer[nodeCount];
    Arrays.fill(counts, loadCount);
    return dist(counts);
  }
  
  private void isBalanced(SimpleAllocatorWrapper w) {
    double mean = (double)w.shards.size() / (double) w.nodes.size();
    int fmean = (int) Math.floor(mean);
    int cmean = (int) Math.ceil(mean);
    
    assertEquals("Should have allocations for every node", w.nodes.size(), w.dist.keySet().size());
    int cmeanCount = 0;
    int fmeanCount = 0;
    for(Collection<Integer> shards : w.dist.asMap().values()) {
      if (shards.size() == fmean) { //if cmean == fmean then we end up in this bucket
        ++fmeanCount; 
      } else {
        assertEquals("Can only be ceiling of floor of the mean shards per node", cmean, shards.size());
        ++cmeanCount;
      }
    }
    assertEquals("Should have remainder count of over allocated", w.shards.size() % w.nodes.size(), cmeanCount);
    assertEquals("All others should have the floor of the mean", w.nodes.size() - cmeanCount, fmeanCount);
  }
  
  @Test
  public void constructorValidation() {
    expectException("Nodes cannot be null", IllegalArgumentException.class
        , () -> { 
          new SimpleAllocator<Integer, Integer>((Collection<Integer>)null, Arrays.asList(0), (Map<Integer, Collection<Integer>>)null
            , () -> { return new HashMap<Integer, Collection<Integer>>(); }, (move) -> { }, 1); 
      });
    expectException("Nodes cannot be empty", IllegalArgumentException.class
        , () -> { 
          new SimpleAllocator<Integer, Integer>(new ArrayList<Integer>(), Arrays.asList(0), (Map<Integer, Collection<Integer>>)null
            , () -> { return new HashMap<Integer, Collection<Integer>>(); }, (move) -> { }, 1); 
      });
    expectException("Shards cannot be null", IllegalArgumentException.class
        , () -> { 
          new SimpleAllocator<Integer, Integer>(Arrays.asList(0), (Collection<Integer>)null, (Map<Integer, Collection<Integer>>)null
            , () -> { return new HashMap<Integer, Collection<Integer>>(); }, (move) -> { }, 1); 
      });
    expectException("Shards cannot be empty", IllegalArgumentException.class
        , () -> { 
          new SimpleAllocator<Integer, Integer>(Arrays.asList(0), new ArrayList<Integer>(), (Map<Integer, Collection<Integer>>)null
            , () -> { return new HashMap<Integer, Collection<Integer>>(); }, (move) -> { }, 1); 
      });
  }
  
  @Test
  public void initialBalancingHappens() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,5)
        , new HashSetValuedHashMap<Integer, Integer>());
    try {      
      Thread.sleep(200);  //TODO: We could have an event when balanced
      assertEquals("Should have 3 nodes in distribution", 3, w.dist.keySet().size());
      isBalanced(w);
    } finally {
      w.close();
    }
  }
  
  @Test
  public void initialBalancingDoesNotHappen() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      Thread.sleep(200);
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      isBalanced(w);
    } finally {
      w.close();
    }
  }
  
  @Test
  public void shouldBalanceOnNewShard() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      Thread.sleep(200);
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.notifyShardChange(integers(0,9));
      Thread.sleep(200);
      assertEquals("Should call distribution discoverer once.", 1, w.discoveryCount.get());
      assertEquals("Should have only one move", 1, w.moveCount.get());
      isBalanced(w);
    } finally {
      w.close();
    }
  }
  
  @Test
  public void shouldBalanceOnLostShard() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      Thread.sleep(200);
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.notifyShardChange(integers(0,7));
      Thread.sleep(200);
      assertEquals("Should call distribution discoverer once.", 1, w.discoveryCount.get());
      assertEquals("Should have only one move", 1, w.moveCount.get());
      isBalanced(w);
    } finally {
      w.close();
    }
  }
  
  @Test
  public void shouldNotBalanceOnRepeatShard() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      Thread.sleep(200);
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.notifyShardChange(integers(0,8));
      Thread.sleep(200);
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      isBalanced(w);
    } finally {
      w.close();
    }
  }
  
  @Test
  public void shouldBalanceOnNewNode() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      Thread.sleep(200);
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.notifyNodeChange(integers(0,3));
      Thread.sleep(200);
      assertEquals("Should call distribution discoverer once.", 1, w.discoveryCount.get());
      assertEquals("Should have 2 moves", 2, w.moveCount.get());
      isBalanced(w);
    } finally {
      w.close();
    }
  }
  
  @Test
  public void shouldBalanceOnLostNode() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      Thread.sleep(200);
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.notifyNodeChange(integers(0,1));
      Thread.sleep(200);
      assertEquals("Should call distribution discoverer once.", 1, w.discoveryCount.get());
      assertEquals("Should have 3 moves", 3, w.moveCount.get());
      isBalanced(w);
    } finally {
      w.close();
    }
  }
}
