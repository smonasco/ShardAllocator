package org.shannon.ConstrainedQueue;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Such a trivial Constrainer, I'm not sure anyone will ever have a need except for for trivial pursuits (tests, etc)
 * 
 * This queue is unfair.  In that FIFO is not guaranteed.
 * 
 * @author Shannon
 *
 * @param <T> That which is being constrained
 */
public class CardinalityConstrainer<T> implements Constrainer<T> {
  private final ArrayBlockingQueue<T> constrained;
  private AtomicInteger released = new AtomicInteger(0);
  private final int maxReleased;
  
  public CardinalityConstrainer(int maxConstrained, int maxReleased) {
    this.constrained = new ArrayBlockingQueue<T>(maxConstrained);
    this.maxReleased = maxReleased;
  }  
  
  private boolean constrained() {
    return released.getAndUpdate((i) -> { return i < maxReleased ? ++i : i; }) < maxReleased;
  }
  
  @Override
  public boolean constrained(T t) throws InterruptedException {
    if (constrained()) {
      return false;
    } else {
      constrained.put(t);
      return true;
    }
  }

  @Override
  public boolean constrained(T t, long time, TimeUnit unit) throws InterruptedException, TimeoutException {
    if (constrained()) {
      return false;
    } else {
      if (!constrained.offer(t, time, unit)) {
        throw new TimeoutException();
      }
      return true;
    }
  }

  @Override
  public Iterable<T> notifyReleased(T t) {
    released.decrementAndGet();
    if(!constrained()) {
      return Arrays.asList(constrained.poll());
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public void clear() {
    //This means that items entering concurrently could be dropped which is probably ok
    //but if we didn't clear constrained after cleaning released we could end up with messages
    //that never clear constrained and that is not ok
    released.set(0);
    constrained.clear();
  }

  @Override
  public boolean isEmpty() {
    return released.get() == 0 && constrained.isEmpty();
  }

  @Override
  public boolean remove(Object o) {
    return constrained.remove(o);
  }

  @Override
  public int size() {
    return constrained.size();
  }

  @Override
  public boolean contains(Object o) {
    return constrained.contains(o);
  }

  @Override
  public int remainingCapacity() {
    return constrained.remainingCapacity();
  }
  
}
