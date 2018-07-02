# ShardAllocator
Whether it be parts of an Index or a set of queues, when control needs to be spread across a cluster's nodes through logical sections, you need a ShardAllocator.

This is different than a load balancer in that a load balancer assigns small amounts of work as it happens, but a ShardAllocator moves ownership of a partition of a whole.

## Assumptions

* Shards do not move without ShardAllocator initiating it.
* The ShardAllocator's owner will tell it when nodes and shards change

**Table of Contents**

[TOCM]

[TOC]

## Design goals

This is an early attempt at making a generic Shard Allocation algorithm. The idea is to abstract the most gnarly parts of ensuring that shards are spread amongst nodes in some balanced way while leaving the implementation of how to actually assign and discover shards and nodes to the user.

This should allow this project to be used equally well for cases that need to cause nodes to listen to a subset of queues or process certain user events or any other grouping of control where it makes more sense to have some affinity between a process and a portion of the whole.

## Example
```java

  private ImmutableSet<Integer> ints(int startInclusive, int endExclusive) {
    return IntStream.range(startInclusive, endExclusive)
            .boxed()
            .collect(ImmutableSet.toImmutableSet());
  }

...

    // We need to balance shards across nodes and as such we need some notion of these entities.
    // In this case, we're just going to play with some Integers, but likely you'll want to have more interesting objects.
    val nodes = ints(1, 3);
    val shards = ints(1, 10);
    val distribution = new HashSetValuedHashMap<Integer, Integer>();
    val sync = new Object();
    val allocator = new SimpleAllocator<Integer, Integer>(
            nodes,
            shards,
            // At this time it is recommended that we provide an already observed distribution.
            // If none is provided, while the solution will work, it is unlikely to be as efficient the first go round.
            // In a future release, it is likely that the ShardAllocator will drive all distribution discovery in the background making this unnecessary.
            /*distribution*/ null,
            // DistributionDiscoverer: some method to discover the distribution
            () -> {
				//Here you would typically hit all the nodes in parallel and ask them what shards they are controlling.
              distribution.keySet().retainAll(nodes);
              return distribution.asMap();
            },
            // ShardRelocator: A call back to move shard ownership. This will be called in
            //    a threaded context, and so it will need to be threadsafe.
            (relocation) -> {
              synchronized(sync) {
			  // Here you'll have to balance between the safety of having 2 nodes try to control the same shard and having some shard have no owner for some amount of time.
                if (relocation.getFromNode() != null) {
					// Usually a relocation will be from one node to another, but when a node leaves or a new shard is detected there will be no from node.
                  distribution.removeMapping(relocation.getFromNode(), relocation.getShard());
                }
                if (relocation.getToNode() != null) {
					// When a shard is removed or a split brain situation happens there may be no node to move ownership to.
                  distribution.put(relocation.getToNode(), relocation.getShard());
                }
              }
            },
            // Despite our best efforts there exists cases where multiple nodes believe they own the same shard. Garbage Collections and other mess have been shown to cause this in the wild.
			// If some amount of state is persisted, resolution could get messy and heavy testing is recommended, and resolution is unclear
			// In other cases, something like the following that removes load from the heavier nodes is recommended.
            (shard, conflictedNodes, nodesByCount) -> {
              return nodesByCount.values().stream()
                      .skip(1)
                      .map(heavyNode -> new ShardRelocation(heavyNode, null, shard))
                      .collect(Collectors.toList());
            }, // Number of threads per node during location
			// Using the idea of a constrained queue, a constraint is put such that no more
			// than this many threads can be operating on a node at the same time
			// In other words, if we need to make a move from node a to b, c to d, e to f and g to e, the moves a to b, c to d and e to f could happen all at the same time and the move g to e would have to wait until e to f finished (or e to f and g to e could happen in reverse order).
			1
    );
```

## Likely Future Improvements

* Optional uses. There are many places where Optional could be used to clean up the interface.
* Stream uses. In the implementation, there are a great many places that could be improved with uses of Stream.
* ShardManager interface. The constructor of a SimpleAllocator has gotten cumbersome. It may be worth having a ShardManager in a Inverse of Control (IoC) like situation. The ShardAllocator would drive distribution discovery and the ShardManager would:
	* supply the various interfaces needed by the ShardAllocator (ShardRelocator, DistributionDiscoverer and SplitBrainResolver)
	* be constructable from supplying the implementation of the above interfaces
* Some implementation to Resolve Split Brain issues by picking the least loaded node
* Some implementaiton to relocate shards given an assignment and release request (probably just some lambda to send a message to take and/or release ownership)
* Some helper functionality to call all nodes simultaneously to ask what they are controlling.