package org.shannon.ConstrainedQueue;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public interface Constrainer<T> {
  /**
   * Returns if the supplied object is constrained.  If it is constrained then the Constrainer should hold it.
   * If the Constrainer is full then it should block until said enters.
   * 
   * @param t The object to be checked to contraints
   * @return  if the supplied object is constrained
   */
  public boolean constrained(T t);
  public boolean constrained(T t, long time, TimeUnit unit);
  /**
   * Notification that the object is leaving the queue.  Should return all objects unconstrained by the released object.
   * @param t The object leaving the queue.
   * @return  All objects unconstrained by the released object.
   */
  public Iterable<T> notifyReleased(T t);
  /**
   * Notification that the queue is going down and we can forget everything
   */
  public void clear();
  public boolean isEmpty();
  /**
   * Removes what is in the collection if present from any constraints.
   * @param collection  The set of objects to remove
   * @return  true if something was removed.
   */
  public boolean removeAll(Collection<?> collection);
  public boolean remove(Object o);
  public boolean retainAll(Collection<?> collection);
  public int size();
  public boolean contains(Object o);
  public int remainingCapacity();
}
