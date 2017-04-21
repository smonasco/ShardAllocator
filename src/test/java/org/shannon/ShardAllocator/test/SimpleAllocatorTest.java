package org.shannon.ShardAllocator.test;

import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.junit.Test;
import org.shannon.ShardAllocator.Impl.SimpleAllocator;
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
    HashSet<Integer> nodes = integers(0,2);
    HashSet<Integer> shards = integers(0,5);
    Object sync = new Object();
    HashSetValuedHashMap<Integer, Integer> dist = new HashSetValuedHashMap<Integer, Integer>();
    
    SimpleAllocator<Integer, Integer> allocator = new SimpleAllocator<Integer, Integer>(nodes, shards, null, () -> {
      return dist.asMap();
    }, (relocation) -> {
      synchronized(sync) {
        if (relocation.getFromNode() != null) { dist.removeMapping(relocation.getFromNode(), relocation.getShard()); }
        if (relocation.getToNode() != null) { dist.put(relocation.getToNode(), relocation.getShard()); }
      }
    }, 3);
    try {      
      Thread.sleep(200);
      assertEquals("Should have 3 nodes in distribution", 3, dist.keySet().size());
      for(Integer node : nodes){
        assertEquals("Should have 2 shards per node", 2, dist.get(node).size());
      }
    } finally {
      allocator.close();
    }
  }
  
  @Test
  public void initialBalancingDoesNotHappen() throws InterruptedException {
    SimpleAllocator<Integer, Integer> allocator = new SimpleAllocator<Integer, Integer>(
      integers(0,2),
      integers(0,8),
      balancedDist(3,3).asMap(),
      () -> { 
        captureException(() -> {
          fail("Should not call distribution discoverer.");
        });
        return null;
      }, (relocation) -> {
        captureException(() -> { fail("Shouldn't be trying to move."); });
      }, 1      
    );
    try {
      Thread.sleep(100);
    } finally {
      allocator.close();
    }
  }
  
  @Test
  public void laterBlancingHappens() {
    //TODO
  }
}
