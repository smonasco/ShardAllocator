package org.shannon.ShardAllocator.Impl;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.shannon.ConstrainedQueue.ConstrainedQueue;
import org.shannon.ConstrainedQueue.ShardRelocationConstrainer;
import org.shannon.ShardAllocator.DistributionDiscoverer;
import org.shannon.ShardAllocator.ShardAllocator;
import org.shannon.ShardAllocator.ShardRelocation;
import org.shannon.ShardAllocator.ShardRelocator;
import org.shannon.ShardAllocator.SplitBrainResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

/**
 * Allocates nodes based on an algorithm that attempts to make the fewest number of moves and assuming equal weight of every
 * Shard.
 * 
 * Internally, use of HashSet is used, so the objects must implement reasonable hashCode and equals functions.
 * 
 * Assumption: It is assumed that if a node is not in our nodeUniverse that any distribution we get for it can be forgotten.
 *   This is further based on the assumption that an unreachable node will release its ownership of a Shard and when it rejoins,
 *   it will not believe itself to be an owner of any Shard.
 * 
 * @author Shannon
 *
 * @param <Node>
 * @param <Shard>
 */
public final class SimpleAllocator<Node, Shard> implements ShardAllocator<Node, Shard> {
  private final static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private ImmutableSet<Node> nodeUniverse;
  private ImmutableSet<Shard> shardUniverse;
  private Map<Node, HashSet<Shard>> distribution;
  private final int maxThreadsPerNode;
  private final ExecutorService parentExecutor = Executors.newSingleThreadExecutor();
  private Future<?> relocationJob;
  private final ShardRelocator<Node, Shard> relocator;
  private final DistributionDiscoverer<Node, Shard> distDiscoverer;
  private final SplitBrainResolver<Node, Shard> splitBrainResolver;
  private boolean balancing = false;
  
  //TODO: This is too cumbersome. We can go multiple ways. Builder pattern or globbing params into objects
  public SimpleAllocator(Collection<Node> nodes, Collection<Shard> shards, Map<Node, Collection<Shard>> distribution
      , DistributionDiscoverer<Node, Shard> distDiscoverer, ShardRelocator<Node, Shard> relocator
      , SplitBrainResolver<Node, Shard> splitBrainResolver, int relocatingThreadsPerNode) {
    Preconditions.checkArgument(nodes != null  && !nodes.isEmpty(), "If you have no nodes, shards cannot be allocated");
    Preconditions.checkArgument(shards != null  && !shards.isEmpty(), "If you have no shards, then shards cannot be allocated");
    Preconditions.checkNotNull(distDiscoverer, "Must have a distDiscoverer");
    Preconditions.checkNotNull(relocator, "Must have a relocator");
    Preconditions.checkNotNull(splitBrainResolver, "Must have a splitBrainResolver");
    nodeUniverse = ImmutableSet.copyOf(nodes);
    shardUniverse = ImmutableSet.copyOf(shards);
    this.distribution = distribution == null ? new HashMap<Node, HashSet<Shard>>() : deepEnoughClone(distribution);
    this.maxThreadsPerNode = relocatingThreadsPerNode;
    this.distDiscoverer = distDiscoverer;
    this.relocator = relocator;
    this.splitBrainResolver = splitBrainResolver;
    allocateAsync();
  }

  private HashMap<Node, HashSet<Shard>> deepEnoughClone(Map<Node, Collection<Shard>> map) {
    HashMap<Node, HashSet<Shard>> retval = new HashMap<Node, HashSet<Shard>>();
    for (Map.Entry<Node, Collection<Shard>> entry : map.entrySet()) {
      retval.put(entry.getKey(), new HashSet<Shard>(entry.getValue()));
    }
    return retval;
  }
  
  @Override
  public void awaitRebalance() {
    while(balancing || !relocationJob.isDone()) {
      try {
        relocationJob.get();
      } catch (InterruptedException | CancellationException e) {
        //Don't care just check again
      } catch (ExecutionException e) {
        logger.error("Caught an ExecutionException trying to relocate. This should not happen.", e);
      }
    }
  }
  
  private synchronized void allocateAsync() {
    balancing = true;
    if (relocationJob != null) { relocationJob.cancel(true); }
    relocationJob = parentExecutor.submit(() -> {
      ArrayList<Future<?>> futures = new ArrayList<Future<?>>();
      final ConstrainedQueue<ShardRelocation<Node, Shard>> moves = determineMoves();
      if(!moves.isEmpty() && !Thread.interrupted()) {
        ExecutorService threadPool = Executors.newFixedThreadPool(nodeUniverse.size() * maxThreadsPerNode);
        try {
          while (!moves.isEmpty()) {
            final ShardRelocation<Node, Shard> move = moves.take();
            futures.add(threadPool.submit(() -> { relocator.relocate(move); moves.forget(move); }));
          }
          for(Future<?> future : futures) {
            future.get();            
          }
        } catch(Throwable e) {
          logger.error("SimpleAllocator.allocateAsync() - Caught Expection while trying to move shards.", e);
          threadPool.shutdownNow();
          //TODO: is 5 minutes good for everyone?  probably OK; we'll loop until we're good.
          try {
            threadPool.awaitTermination(5, TimeUnit.MINUTES);
          } catch (InterruptedException e1) {
            logger.warn("SimpleAllocator.allocateAsync() - Captured InterruptExcpetion", e1);
          }
        }
        discoverDistribution();
        allocateAsync();
      } else {
        balancing = false;
      }
    });
  }

  private void discoverDistribution() {
    distribution = deepEnoughClone(distDiscoverer.discoverDistribution());
  }
  
  private TreeMultimap<Integer, Node> nodesByCount() {
    TreeMultimap<Integer, Node> retval = TreeMultimap.create(Ordering.natural(), Ordering.arbitrary());
    for(Entry<Node, HashSet<Shard>> entry : distribution.entrySet()) {
      retval.put(entry.getValue().size(), entry.getKey());
    }
    Sets.difference(nodeUniverse, new HashSet<Node>(retval.values())).forEach((node) -> { retval.put(0, node); });
    return retval;
  }
  
  private <K, V> Pair<K, V> getFirst(TreeMultimap<K, V> map) {
    Map.Entry<K, Collection<V>> entry = map.asMap().firstEntry();
    return Pair.of(entry.getKey(), ((SortedSet<V>)(entry.getValue())).first());
  }
  
  private <K, V> Pair<K, V> getLast(TreeMultimap<K, V> map) {
    Map.Entry<K, Collection<V>> entry = map.asMap().lastEntry();
    return Pair.of(entry.getKey(), ((SortedSet<V>)(entry.getValue())).last());
  }
  
  private void assignToLeast(Shard shard, TreeMultimap<Integer, Node> nodesByCount, ConstrainedQueue<ShardRelocation<Node, Shard>> moves
      , Map.Entry<Integer, Node> fromEntry) {
    Map.Entry<Integer, Node> leastEntry = getFirst(nodesByCount);
    moves.add(new ShardRelocation<Node, Shard>(fromEntry == null ? null : fromEntry.getValue(), leastEntry.getValue(), shard));
    if (fromEntry != null) {
      nodesByCount.remove(fromEntry.getKey(), fromEntry.getValue());
      nodesByCount.put(fromEntry.getKey() - 1, fromEntry.getValue());
      distribution.get(fromEntry.getValue()).remove(shard);
    }
    nodesByCount.remove(leastEntry.getKey(), leastEntry.getValue());
    nodesByCount.put(leastEntry.getKey() + 1, leastEntry.getValue());
  }
  
  private void allShardsAccountedFor(ConstrainedQueue<ShardRelocation<Node, Shard>> moves, TreeMultimap<Integer, Node> nodesByCount) {
    HashSet<Shard> unassignedLoad = new HashSet<Shard>(shardUniverse);
    distribution.values().forEach((shards) -> { unassignedLoad.removeAll(shards); } );
    
    unassignedLoad.forEach((shard) -> { assignToLeast(shard, nodesByCount, moves, null); });
  }
  
  private void allNodesEven(ConstrainedQueue<ShardRelocation<Node, Shard>> moves, TreeMultimap<Integer, Node> nodesByCount
      , int maxShardsPerNode, int minShardsPerNode) {
    Map.Entry<Integer, Node> lastEntry;
    while((lastEntry = getLast(nodesByCount)).getKey() > maxShardsPerNode 
        || getFirst(nodesByCount).getKey() < minShardsPerNode) {
      assignToLeast(distribution.get(lastEntry.getValue()).iterator().next(), nodesByCount, moves, lastEntry);
    }
  }
  
  private void fillInMissingNodes() {
    for (Node node : Sets.difference(nodeUniverse, distribution.keySet())) {
      distribution.put(node, new HashSet<Shard>());
    }
  }
  
  private void removeLeavers(ConstrainedQueue<ShardRelocation<Node, Shard>> moves) {
    HashSet<Node> leavingNodes = new HashSet<Node>();
    ArrayList<Runnable> actions = new ArrayList<Runnable>();
    for (Map.Entry<Node, HashSet<Shard>> entry : distribution.entrySet()) {
      if (nodeUniverse.contains(entry.getKey())) {
        for (Shard shard : Sets.difference(entry.getValue(), shardUniverse)) {
          moves.add(new ShardRelocation<Node, Shard>(entry.getKey(), null, shard));
          actions.add(() -> { distribution.get(entry.getKey()).remove(shard); });
        }
      } else {
        leavingNodes.add(entry.getKey());
      }
    }
    actions.forEach((action) -> { action.run(); });
    //Assuming that the nodes left. A node should not be able to join and have ownership of a Shard without going through this.
    leavingNodes.forEach((node) -> { distribution.remove(node); });
  }
  
  private boolean handleSplitBrain(ConstrainedQueue<ShardRelocation<Node, Shard>> moves
      , TreeMultimap<Integer, Node> nodesByCount) {
    boolean haveNewMoves = false;
    
    HashSetValuedHashMap<Shard, Node> nodesByShard = new HashSetValuedHashMap<Shard, Node>();
    for(Map.Entry<Node, HashSet<Shard>> entry : distribution.entrySet()) {
      for(Shard shard : entry.getValue()) {
        nodesByShard.put(shard, entry.getKey());
      }
    }
    
    for(Map.Entry<Shard, Collection<Node>> entry : nodesByShard.asMap().entrySet()) {
      if (entry.getValue().size() > 1) {
        Collection<ShardRelocation<Node, Shard>> newMoves = 
            splitBrainResolver.resolve(entry.getKey(), new HashSet<Node>(entry.getValue()), nodesByCount);
        if (newMoves != null && !newMoves.isEmpty()) {
          haveNewMoves = true;
          moves.addAll(newMoves);
        }
      }
    }
    
    return haveNewMoves;
  }
  
  private ConstrainedQueue<ShardRelocation<Node, Shard>> determineMoves() {
    ConstrainedQueue<ShardRelocation<Node, Shard>> moves = new ConstrainedQueue<ShardRelocation<Node, Shard>>(
      new ShardRelocationConstrainer<Node, Shard>(maxThreadsPerNode),
      new LinkedBlockingQueue<ShardRelocation<Node, Shard>>()
    );
    double mean = (double) shardUniverse.size() / (double) nodeUniverse.size();
    int cMean = (int) Math.ceil(mean);
    int fMean = (int) Math.floor(mean);
    
    fillInMissingNodes();
    removeLeavers(moves);
    TreeMultimap<Integer, Node> nodesByCount = nodesByCount();
    //If we call splitBrainResolver.resolve and it gives us moves
    //then we should honor that and not reassign those shard(s)
    if (!handleSplitBrain(moves, nodesByCount)) {
      allShardsAccountedFor(moves, nodesByCount);
      allNodesEven(moves, nodesByCount, cMean, fMean);
    }    
    return moves;
  }

  @Override
  public void notifyShardsChange(Collection<Shard> shards) {
    this.shardUniverse = ImmutableSet.copyOf(shards);
    allocateAsync();
  }

  @Override
  public void notifyNodesChange(Collection<Node> nodeUniverse) {
    this.nodeUniverse = ImmutableSet.copyOf(nodeUniverse);
    allocateAsync();    
  }

  @Override
  public void notifyDistributionChange(Map<Node, Collection<Shard>> distribution) {
    this.distribution = distribution == null ? new HashMap<Node, HashSet<Shard>>() : deepEnoughClone(distribution);
    allocateAsync();
  }
  
  @Override
  public void close() {
    if (!relocationJob.isDone()) {
      relocationJob.cancel(true);
      try {
        relocationJob.get();
      } catch (InterruptedException | ExecutionException | CancellationException e) {
        logger.info("SimpleAllocator.close - Exception caught closing. Assuming all is done.", e);
      }
    }
  }
}
