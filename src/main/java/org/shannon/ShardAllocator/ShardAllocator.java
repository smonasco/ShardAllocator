package org.shannon.ShardAllocator;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;

/**
 * Allocates Shards between nodes based on some idea of balance.
 *
 * @param <Node>    That which controls or has ownership of Shards
 * @param <Shard>   Some fragment of the whole which needs controlling.
 */
public interface ShardAllocator<Node, Shard> extends Closeable{
  /**
   * Called when the universe of shards changes. Will kick off a rebalance if not balanced.
   *
   * @param shardUniverse   All the shards.
   */
  void notifyShardsChange(Collection<Shard> shardUniverse);

  /**
   * Called when the universe of nodes changes. Will kick off a rebalance if not balanced.
   *
   * @param nodeUniverse    All the nodes.
   */
  void notifyNodesChange(Collection<Node> nodeUniverse);

  /**
   * Called when the distribution changes. Will kick off a rebalance if not balanced.
   *
   * @param distribution    Mapping of nodes to the shards that they control.
   */
  void notifyDistributionChange(Map<Node, Collection<Shard>> distribution);

  /**
   * If a rebalance is going on this will block until it is done.
   */
  void awaitRebalance();
}
