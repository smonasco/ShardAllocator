# ShardAllocator
Whether it be parts of an Index or a set of queues, when your work needs to be spread across a cluster's nodes through logical sections, you need a ShardAllocator.

This is different than a load balancer in that a load balancer assigns small amounts of work as it happens, but a ShardAllocator moves ownership of a partition of a whole.

## Assumptions

* Shards do not move without ShardAllocator initiating it.
* The ShardAllocator's owner will tell it when nodes and shards change
