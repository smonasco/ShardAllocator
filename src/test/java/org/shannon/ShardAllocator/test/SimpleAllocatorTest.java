package org.shannon.ShardAllocator.test;

import com.google.common.collect.ImmutableSet;
import lombok.val;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.junit.Test;
import org.shannon.ShardAllocator.Impl.SimpleAllocator;
import org.shannon.ShardAllocator.ShardRelocation;
import org.shannon.ShardAllocator.mock.SimpleAllocatorWrapper;
import org.shannon.util.TestClass;

import static org.junit.Assert.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
  
  @Test
  public void constructorValidation() {
    expectException("Nodes cannot be null", IllegalArgumentException.class
        , () -> { 
          new SimpleAllocator<Integer, Integer>((Collection<Integer>)null, Arrays.asList(0), (Map<Integer, Collection<Integer>>)null
            , () -> { return new HashMap<Integer, Collection<Integer>>(); }, (move) -> { }
            , (shard, nodes, count) -> { return null; }, 1); 
      });
    expectException("Nodes cannot be empty", IllegalArgumentException.class
        , () -> { 
          new SimpleAllocator<Integer, Integer>(new ArrayList<Integer>(), Arrays.asList(0), (Map<Integer, Collection<Integer>>)null
            , () -> { return new HashMap<Integer, Collection<Integer>>(); }, (move) -> { }
            , (shard, nodes, count) -> { return null; }, 1); 
      });
    expectException("Shards cannot be null", IllegalArgumentException.class
        , () -> { 
          new SimpleAllocator<Integer, Integer>(Arrays.asList(0), (Collection<Integer>)null, (Map<Integer, Collection<Integer>>)null
            , () -> { return new HashMap<Integer, Collection<Integer>>(); }, (move) -> { }
            , (shard, nodes, count) -> { return null; }, 1); 
      });
    expectException("Shards cannot be empty", IllegalArgumentException.class
        , () -> { 
          new SimpleAllocator<Integer, Integer>(Arrays.asList(0), new ArrayList<Integer>(), (Map<Integer, Collection<Integer>>)null
            , () -> { return new HashMap<Integer, Collection<Integer>>(); }, (move) -> { }
            , (shard, nodes, count) -> { return null; }, 1); 
      });
    expectException("Must have a distDiscoverer", IllegalArgumentException.class
        , () -> { 
          new SimpleAllocator<Integer, Integer>(Arrays.asList(0), new ArrayList<Integer>(), (Map<Integer, Collection<Integer>>)null
            , null, (move) -> { }
            , (shard, nodes, count) -> { return null; }, 1); 
      });
    expectException("Must have a relocator", IllegalArgumentException.class
        , () -> { 
          new SimpleAllocator<Integer, Integer>(Arrays.asList(0), new ArrayList<Integer>(), (Map<Integer, Collection<Integer>>)null
            , () -> { return new HashMap<Integer, Collection<Integer>>(); }, null
            , (shard, nodes, count) -> { return null; }, 1); 
      });
    expectException("Must have a splitBrainResolver", IllegalArgumentException.class
        , () -> { 
          new SimpleAllocator<Integer, Integer>(Arrays.asList(0), new ArrayList<Integer>(), (Map<Integer, Collection<Integer>>)null
            , () -> { return new HashMap<Integer, Collection<Integer>>(); }, (move) -> { }
            , null, 1); 
      });
  }

  @Test
  public void initialBalancingHappens() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,5)
        , new HashSetValuedHashMap<Integer, Integer>());
    try {      
      w.awaitRebalance();
      assertEquals("Should have 3 nodes in distribution", 3, w.dist.keySet().size());
      w.isBalanced();
    } finally {
      w.close();
    }
  }
  
  @Test
  public void initialBalancingDoesNotHappen() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      w.awaitRebalance();
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.isBalanced();
    } finally {
      w.close();
    }
  }
  
  @Test
  public void shouldBalanceOnNewShard() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      w.awaitRebalance();
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.notifyShardChange(integers(0,9));
      w.awaitRebalance();
      assertEquals("Should call distribution discoverer once.", 1, w.discoveryCount.get());
      assertEquals("Should have only one move", 1, w.moveCount.get());
      w.isBalanced();
    } finally {
      w.close();
    }
  }
  
  @Test
  public void shouldBalanceOnLostShard() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      w.awaitRebalance();
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.notifyShardChange(integers(0,7));
      w.awaitRebalance();
      assertEquals("Should call distribution discoverer once.", 1, w.discoveryCount.get());
      assertEquals("Should have only one move", 1, w.moveCount.get());
      w.isBalanced();
    } finally {
      w.close();
    }
  }
  
  @Test
  public void shouldNotBalanceOnRepeatShard() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      w.awaitRebalance();
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.notifyShardChange(integers(0,8));
      w.awaitRebalance();
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.isBalanced();
    } finally {
      w.close();
    }
  }
  
  @Test
  public void shouldBalanceOnNewNode() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      w.awaitRebalance();
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.notifyNodeChange(integers(0,3));
      w.awaitRebalance();
      assertEquals("Should call distribution discoverer once.", 1, w.discoveryCount.get());
      assertEquals("Should have 2 moves", 2, w.moveCount.get());
      w.isBalanced();
    } finally {
      w.close();
    }
  }
  
  @Test
  public void shouldBalanceOnLostNode() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      w.awaitRebalance();
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.notifyNodeChange(integers(0,1));
      w.awaitRebalance();
      assertEquals("Should call distribution discoverer once.", 1, w.discoveryCount.get());
      assertEquals("Should have 3 moves", 3, w.moveCount.get());
      w.isBalanced();
    } finally {
      w.close();
    }
  }
  
  @Test
  public void shouldNotBalanceOnRepeatNode() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      w.awaitRebalance();
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.notifyNodeChange(integers(0,2));
      w.awaitRebalance();
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.isBalanced();
    } finally {
      w.close();
    }
  }
  
  @Test
  public void shouldBalanceOnBadDistribution() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      w.awaitRebalance();
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.notifyDistributionChange(dist(3,3,2));
      w.awaitRebalance();
      assertEquals("Should call distribution discoverer once.", 1, w.discoveryCount.get());
      assertEquals("Should have 1 moves", 1, w.moveCount.get());
      w.isBalanced();
    } finally {
      w.close();
    }
  }
  
  @Test
  public void shouldNotBalanceOnGoodDistribution() throws InterruptedException {
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      w.awaitRebalance();
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.notifyDistributionChange(balancedDist(3,3));
      w.awaitRebalance();
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      w.isBalanced();
    } finally {
      w.close();
    }
  }

  @Test
  public void shouldBalanceRandomness() throws InterruptedException {
    Random rand = new Random();
    SimpleAllocatorWrapper w = new SimpleAllocatorWrapper(integers(0,2), integers(0,8), balancedDist(3,3));
    try {
      w.awaitRebalance();
      assertEquals("Shouldn't call distribution discoverer.", 0, w.discoveryCount.get());
      assertEquals("Shouldn't call relocation", 0, w.moveCount.get());
      for (int i = 0; i < 100; ++i) {
        System.out.println("Random test " + i);
        switch(rand.nextInt(3)) {
          case 0:
            w.notifyNodeChange(integers(0, rand.nextInt(100) + 1));
            break;
          case 1:
            w.notifyShardChange(integers(0, rand.nextInt(100) + 1));
            break;
          case 2:
            ArrayList<Integer> counts = new ArrayList<Integer>();
            for(@SuppressWarnings("unused") Integer j : integers(0, rand.nextInt(100))) {
              counts.add(rand.nextInt(100));
            }
            w.notifyDistributionChange(dist(counts.toArray(new Integer[0])));
            break;
        }
        w.awaitRebalance();
        w.isBalanced();
      }
    } finally {
      w.close();
    }
  }
}
