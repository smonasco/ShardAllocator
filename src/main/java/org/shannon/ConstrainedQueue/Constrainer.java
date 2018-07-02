package org.shannon.ConstrainedQueue;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface Constrainer<T> {
  /**
   * Returns if the supplied object is constrained.  If it is constrained then the Constrainer should hold it.
   * If the Constrainer is full then it should block until it can be held.
   * 
   * @param t The object to be checked to contraints
   * @return  if the supplied object is constrained
   * @throws InterruptedException 
   */
  boolean constrained(T t) throws InterruptedException;

  /**
   * Returns if the supplied object is constrained.  If it is constrained then the Constrainer should hold it.
   * If the Constrainer is full then it should block until it can be held or a timeout occurs.
   *
   * @param t The object to be checked to contraints
   * @param time    The number of TimeUnits until a timeout would occur
   * @param unit    The TimeUnit of the timeout.
   * @return  if the supplied object is constrained
   * @throws InterruptedException
   * @throws TimeoutException
   */
  boolean constrained(T t, long time, TimeUnit unit) throws InterruptedException, TimeoutException;
  /**
   * Notification that the object is leaving the queue.  Should return all objects unconstrained by the released object.
   * @param t The object leaving the queue.
   * @return  All objects unconstrained by the released object.
   */
  Collection<T> notifyReleased(T t);
  /**
   * Forget everything
   */
  void clear();
  /**
   * Check if anything is being restrained/held back
   * 
   * @return if anything is currently being held back
   */
  boolean isEmpty();
  boolean remove(Object o);
  int size();
  boolean contains(Object o);
  int remainingCapacity();
}
