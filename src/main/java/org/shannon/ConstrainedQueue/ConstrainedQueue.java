package org.shannon.ConstrainedQueue;

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A ConstrainedQueue is a queue with constraints around what can be released downstream.
 * 
 * An example would be where you need to ensure that within a universe of work only n items
 * with a particular property may be released.  Say you can only have 5 apples in any given basket.
 * 
 * This is a pretty generically built class.  The Blocking of this queue is supplied via 
 * the supplied delegate + {@link Constrainer Constrainer).  The two pieces should individually
 * apply any space constraints (like having an ArrayBlockingQueue for the delegate and having a
 * similar piece in the Constrainer to throttle itself.)
 * 
 * Any space constraints on the delegate and the {@link Constrainer Constrainer) should play nice together.
 * If the {@link Constrainer Constrainer) allows for more items to go through than the delegate allows to entry,
 * then items may back up in an internal Queue when items leaving the delegate cause items to be unconstrained but
 * cannot yet enter the delegate.
 * 
 * forget must be called after work is done.  The idea of a constrained queue is to control how many things are out
 * in the wild and so a feedback as to what can be forgotten about must occur.
 * 
 * @author Shannon
 *
 * @param <T>
 */
public class ConstrainedQueue<T> implements BlockingQueue<T>, Closeable {
  
  private final BlockingQueue<T> delegate;
  private final Constrainer<T> constrainer;
  private final LinkedBlockingQueue<T> trafficJam = new LinkedBlockingQueue<T>();
  private Thread jamClearer;
  private boolean open = true;
  
  public ConstrainedQueue(Constrainer<T> constrainer, BlockingQueue<T> delegate) {
    this.constrainer = constrainer;
    this.delegate = delegate;
    jamClearer = startClearingJams();
  }
  
  private Thread startClearingJams() {
    Thread retval = new Thread(() -> {
      while(true) {
        try {
          if (!open) { break; }
          delegate.put(trafficJam.take());
        } catch (InterruptedException e) {
           //TODO: log
          break;
        }
      }
    });
    retval.start();
    return retval;
  }
  
  public synchronized void open() {
    if(!open) {
      open = true;
      jamClearer = startClearingJams();
    }
  }
  
  public synchronized void close() {
    if (open) {
      open = false;
      clear();
      //oddly some BlockingQueues don't throw InterruptedException immediately if already interrupted
      while(!jamClearer.isInterrupted()) {
        jamClearer.interrupt();
        try {
          jamClearer.join(1); //Should be near immediate
        } catch (InterruptedException e) {
          //Just keep on trying to interrupt
        }
      }
    }
  }
  
  public T forget(T forgotten) {
    if (forgotten != null) {
      for(T unconstrained : constrainer.notifyReleased(forgotten)) {
        if (unconstrained != null) {
          if (!delegate.offer(unconstrained)) {
            trafficJam.offer(unconstrained);
          }
        }
      }
    }
    return forgotten;
  }
  
  @Override
  public T element() {
    return delegate.element();
  }

  @Override
  public T peek() {
    return delegate.peek();
  }

  @Override
  public T poll() {
    return delegate.poll();
  }

  @Override
  public T remove() {
    return delegate.remove();
  }

  @Override
  public boolean addAll(Collection<? extends T> collection) {
    collection.forEach((t) -> { add(t); });
    return !collection.isEmpty();
  }

  @Override
  public synchronized void clear() {
    trafficJam.clear();
    delegate.clear();
    constrainer.clear();
  }

  /**
   * Not implemented.  Constrained items are potentially difficult to find.
   */
  @Override
  public boolean containsAll(Collection<?> collection) {
    throw new UnsupportedOperationException("containsAll is not implemented");
  }

  @Override
  public boolean isEmpty() {
    return constrainer.isEmpty() && delegate.isEmpty() && trafficJam.isEmpty();
  }

  @Override
  public Iterator<T> iterator() {
    return delegate.iterator();
  }

  @Override
  public boolean removeAll(Collection<?> collection) {
    return 
        constrainer.removeAll(collection)
        | delegate.removeAll(collection)
        | trafficJam.removeAll(collection);
  }

  @Override
  public boolean retainAll(Collection<?> collection) {
    return
        constrainer.retainAll(collection)
        | delegate.retainAll(collection)
        | trafficJam.retainAll(collection);
  }

  @Override
  public int size() {
    return
        constrainer.size()
        + delegate.size()
        + trafficJam.size();
  }

  /**
   * Not implemented
   */
  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException("toArray() is not implemented");
  }

  /**
   * Not implemented
   */
  @Override
  public <J> J[] toArray(J[] arg0) {
    throw new UnsupportedOperationException("toArray([]) is not implemented");
  }

  @Override
  public boolean add(T t) {
    if (!constrainer.constrained(t)) {
      add(t);
    }
    return true;
  }

  @Override
  public boolean contains(Object o) {
    return
        delegate.contains(o)
        || trafficJam.contains(o)
        || constrainer.contains(0);
  }

  @Override
  public int drainTo(Collection<? super T> collection) {
    return delegate.drainTo(collection);
  }

  @Override
  public int drainTo(Collection<? super T> c, int maxElements) {
    return delegate.drainTo(c, maxElements);
  }

  /**
   * Always returns true and may have to block in entering Constrainer.
   */
  @Override
  public boolean offer(T t) {
    if (!constrainer.constrained(t)) {
      if(!delegate.offer(t)) {
        trafficJam.offer(t);
      }
    }
    return true;
  }

  @Override
  public boolean offer(T t, long time, TimeUnit unit) throws InterruptedException {
    if(!constrainer.constrained(t, time, unit)) {
      if (!delegate.offer(t)) {
        trafficJam.offer(t);
      }
    }
    return true;
  }

  @Override
  public T poll(long timeout, TimeUnit unit) throws InterruptedException {
    return delegate.poll(timeout, unit);
  }

  @Override
  public void put(T t) throws InterruptedException {
    if (!constrainer.constrained(t)) {
      delegate.put(t);
    }
  }

  @Override
  public int remainingCapacity() {
    long retval = constrainer.remainingCapacity() + delegate.remainingCapacity();
    return (int) Math.min(retval, Integer.MAX_VALUE);
  }

  @Override
  public boolean remove(Object o) {
    return
        constrainer.remove(o)
        | trafficJam.remove(o)
        | delegate.remove(o);
  }

  @Override
  public T take() throws InterruptedException {
    return delegate.take();
  }

}
