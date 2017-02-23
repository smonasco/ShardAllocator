package org.shannon.ShardAllocator;

import com.google.common.base.Preconditions;

/**
 * This describe the relocation that needs to happen.
 * 
 * We have a shard that needs to move, be removed (toNode will be null) or start being worked (fromNode will be null)
 * 
 * @author Shannon
 *
 * @param <Node>
 * @param <Shard>
 */
public class ShardRelocation<Node, Shard> {
  private final Node fromNode;
  private final Node toNode;
  private final Shard shard;
  
  public ShardRelocation(Node fromNode, Node toNode, Shard shard) {
    Preconditions.checkNotNull(shard, "Must have a shard to move.");
    this.fromNode = fromNode;
    this.toNode = toNode;
    this.shard = shard;
  }

  /**
   * The node that needs to let go of ownership of the shard.  Will be null if no node needs to let go (shard is new)
   * 
   * @return  The node that needs to let go of ownership of the node.  Will be null if no node needs to let go (shard is new)
   */
  public Node getFromNode() {
    return fromNode;
  }

  /**
   * The node that needs to take ownership of the shard.  Will be null of it needs to go nowhere (shard is destroyed)
   * 
   * @return  The node that needs to take ownership of the shard.  Will be null of it needs to go nowhere (shard is destroyed)
   */
  public Node getToNode() {
    return toNode;
  }

  /**
   * That which needs to be relocated.
   * 
   * @return  That which needs to be relocated.
   */
  public Shard getShard() {
    return shard;
  }
}
