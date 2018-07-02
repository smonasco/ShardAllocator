package org.shannon.ShardAllocator;

import com.google.common.base.Preconditions;

/**
 * This describes the relocation that needs to happen.
 * 
 * We have a shard that needs to move, be removed (toNode will be null) or start being worked (fromNode will be null)
 * 
 * @author Shannon
 *
 * @param <Node>    That which controls or has ownership of Shards
 * @param <Shard>   Some fragment of the whole which needs controlling.
 */
public class ShardRelocation<Node, Shard> {
  private final Node fromNode;
  private final Node toNode;
  private final Shard shard;

  /**
   * This describes the relocation that needs to happen.
   *
   * We have a shard that needs to move, be removed (toNode will be null) or start being worked (fromNode will be null)
   *
   * @param fromNode  Node that needs to release control if any.
   * @param toNode    Node that needs to receive control if any.
   * @param shard     Shard that needs to have its ownership changed.
   */
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
  
  private boolean eq(Object l, Object r) {
    return (l == null && r == null) || (l != null && l.equals(r));
  }
  
  @Override
  public boolean equals(Object o) {
    return o instanceof ShardRelocation<?, ?>
      && equals((ShardRelocation<?, ?>)o);
  }
  
  public boolean equals(ShardRelocation<?, ?> o) {
    return eq(o.fromNode, fromNode)
        && eq(o.toNode, toNode)
        && o.shard.equals(shard);
  }
  
  private int hashCode(Object o) {
    return o == null ? 0 : o.hashCode();
  }
  
  @Override
  public int hashCode() {
    return hashCode(fromNode) ^ hashCode(toNode) ^ shard.hashCode();
  }
  
  private String toStringJSON(Object o) {
    return o == null ? "NULL" : String.format("\"%s\"", o);
  }
  
  @Override
  public String toString() {
    return String.format("{ \"fromNode\": %s, \"toNode\": %s, \"shard\": %s }"
        , toStringJSON(fromNode), toStringJSON(toNode), toStringJSON(shard));
  }
}
