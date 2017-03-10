package org.shannon.ConstrainedQueue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.shannon.ShardAllocator.ShardRelocation;

public class ShardRelocationConstrainer<Node, Shard> implements Constrainer<ShardRelocation<Node, Shard>> {
  private HashSetValuedHashMap<Node, ShardRelocation<Node, Shard>> waitLists = new HashSetValuedHashMap<Node, ShardRelocation<Node, Shard>>();
  private HashMap<Node, Integer> activeCounts = new HashMap<Node, Integer>();
  private final int maxThreadsPerNode;
  
  public ShardRelocationConstrainer(int maxThreadsPerNode) {
    this.maxThreadsPerNode = maxThreadsPerNode;
  }
  
  private synchronized boolean constrained(Node n, ShardRelocation<Node, Shard> e) {
    Integer activeCount = activeCounts.getOrDefault(n, 0);
    if (activeCount == maxThreadsPerNode) {
      waitLists.put(n, e);
      return true;
    } else {
      return false;
    }    
  }
  
  private synchronized void incrementActiveCount(Node n) {
    activeCounts.merge(n, 1, (oldValue, newValue) -> { return ++oldValue; });
  }
  
  private synchronized void decrementActiveCount(Node n) {
    activeCounts.computeIfPresent(n, (key, oldValue) -> { return oldValue == 1 ? null : --oldValue; });
  }

  @Override
  public synchronized boolean constrained(ShardRelocation<Node, Shard> e) {
    if (!constrained(e.getFromNode(), e) && !constrained(e.getToNode(), e)) {
      incrementActiveCount(e.getFromNode());
      incrementActiveCount(e.getToNode());
      return false;
    } else {
      return true;
    }
  }

  @Override
  public boolean constrained(ShardRelocation<Node, Shard> e, long time, TimeUnit unit) {
    return constrained(e);  //If a lot of threads are calling this could block I guess, but...
  }

  private synchronized void release(Node n, ArrayList<ShardRelocation<Node, Shard>> released) {
    decrementActiveCount(n);
    Set<ShardRelocation<Node, Shard>> waitList = waitLists.get(n);
    for(ShardRelocation<Node, Shard> relocation : waitList) {
      if(!constrained(relocation)) {
        released.add(relocation);
        waitList.remove(relocation);
        if (waitList.isEmpty()) {
          waitLists.remove(n);
        }
        return;
      }
    }
  }
  
  @Override
  public synchronized Collection<ShardRelocation<Node, Shard>> notifyReleased(ShardRelocation<Node, Shard> e) {
    ArrayList<ShardRelocation<Node, Shard>> retval = new ArrayList<ShardRelocation<Node, Shard>>();
    release(e.getFromNode(), retval);
    release(e.getToNode(), retval);
    return retval;
  }

  @Override
  public synchronized void clear() {
    waitLists.clear();
    activeCounts.clear();
  }

  @Override
  public synchronized boolean isEmpty() {
    return waitLists.isEmpty();
  }

  private synchronized boolean modifyAllWaitLists(Function<Collection<ShardRelocation<Node, Shard>>, Boolean> actor) {
    HashSet<Node> keysToRemove = new HashSet<Node>();
    boolean retval = false;
    for(Map.Entry<Node, Collection<ShardRelocation<Node, Shard>>> entry : waitLists.asMap().entrySet()) {
      retval |= actor.apply(entry.getValue());
      if (entry.getValue().isEmpty()) {
        keysToRemove.add(entry.getKey());
      }
    }
    keysToRemove.forEach((node) -> { waitLists.remove(node); } );
    return retval;
  }
 
  @Override
  public boolean remove(Object o) {
    return modifyAllWaitLists((set) -> { return set.remove(o); });
  }

  @Override
  public int size() {
    return waitLists.size();
  }

  @Override
  public boolean contains(Object o) {
    return waitLists.containsValue(o);
  }

  @Override
  public int remainingCapacity() {
    return Integer.MAX_VALUE;
  }  

}
