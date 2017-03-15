package org.shannon.ConstrainedQueue;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface Constrainer<T> {
  /**
   * Returns if the supplied object is constrained.  If it is constrained then the Constrainer should hold it.
   * If the Constrainer is full then it should block until said enters.
   * 
   * @param t The object to be checked to contraints
   * @return  if the supplied object is constrained
   * @throws InterruptedException 
   */
  public boolean constrained(T t) throws InterruptedException;
  public boolean constrained(T t, long time, TimeUnit unit) throws InterruptedException, TimeoutException;
  /**
   * Notification that the object is leaving the queue.  Should return all objects unconstrained by the released object.
   * @param t The object leaving the queue.
   * @return  All objects unconstrained by the released object.
   */
  public Collection<T> notifyReleased(T t);
  /**
   * Forget everything
   */
  public void clear();
  /**
   * Check if anything is being restrained/held back
   * 
   * @return if anything is currently being held back
   */
  public boolean isEmpty();
  public boolean remove(Object o);
  public int size();
  public boolean contains(Object o);
  public int remainingCapacity();
}
