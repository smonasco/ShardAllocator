package org.shannon.ConstrainedQueue.Test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.shannon.ConstrainedQueue.CardinalityConstrainer;
import org.shannon.ConstrainedQueue.ConstrainedQueue;
import org.shannon.function.ExceptionalActor;

public class ConstrainedQueueTest {
    
  private static <T extends Throwable> void doNTimes(int n, ExceptionalActor<T> actor) throws T {
    for(;--n != -1;) {
      actor.act();
    }
  }
  
  private ConstrainedQueue<Integer> smallQueue() {
    return new ConstrainedQueue<Integer>(new CardinalityConstrainer<Integer>(5000, 1)
        , new ArrayBlockingQueue<Integer>(1));
  }
  
  private <T extends Throwable> void expectException(String message, Class<T> type, ExceptionalActor<T> actor) {
    Throwable t = null;
    try {
      actor.act();
    } catch (Throwable e) {
      if (e.getClass() == type) {
        t = e;
      }
    }
    assertNotNull(message, t);
  }
  
  @Test
  public void offeredAnItemItCanBePolled() {
    ConstrainedQueue<Integer> q = smallQueue();
    assertTrue("Should be able to offer.", q.offer(0));
    assertEquals("Should get back what I put in.", new Integer(0), q.forget(q.poll()));
    q.close();
  }
  
  @Test
  public void offeredManyItemsTheyCanBePolled() {
    ConstrainedQueue<Integer> q = new ConstrainedQueue<Integer>(new CardinalityConstrainer<Integer>(5000000, 1)
        , new ArrayBlockingQueue<Integer>(1));
    Integer nonce = new Integer(0);
    doNTimes(5000000, () -> { assertTrue("Should be able to offer.", q.offer(nonce)); });
    doNTimes(5000000, () -> { assertEquals("Should get back what I put in.", nonce, q.forget(q.poll())); });
    q.close();
  }
  
  @Test
  public void only1ItemAvailableConcurrently() throws InterruptedException, ExecutionException {
    final ConstrainedQueue<Integer> q = new ConstrainedQueue<Integer>(new CardinalityConstrainer<Integer>(5000000, 1)
        , new ArrayBlockingQueue<Integer>(10));
    try {
      final AtomicBoolean alreadyGotOne = new AtomicBoolean(false);
      final AtomicBoolean failed = new AtomicBoolean(false);
      Integer nonce = new Integer(0);
      doNTimes(5000000, () -> { assertTrue("Should be able to offer.", q.offer(nonce)); });
      ArrayList<CompletableFuture<Void>> futures = new ArrayList<CompletableFuture<Void>>();
      doNTimes(20, () -> { 
        futures.add(CompletableFuture.runAsync(() -> { 
          try {
            Integer item = q.take();
            if (alreadyGotOne.getAndSet(true)) {
              failed.set(true);
              System.out.println("Already had one.");
              futures.forEach((future) -> { future.complete(null); } );
            }
            Thread.sleep(10);
            alreadyGotOne.set(false);
            q.forget(item);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        })); 
      });
      CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()])).get();
      assertFalse("Shouldn't have failed", failed.get());
    } finally {
      q.close();
    }
  }
  
  @Test
  public void element() {
    ConstrainedQueue<Integer> q = smallQueue();
    assertTrue("Should be able to offer.", q.offer(0));
    assertEquals("Should see what I put in.", new Integer(0), q.element());
    assertEquals("Should see get what put in.", new Integer(0), q.forget(q.poll()));
    expectException("Should have NoSuchElementException", NoSuchElementException.class, () -> { q.element(); });
    q.close();
  }
  
  @Test
  public void peek() {
    ConstrainedQueue<Integer> q = smallQueue();
    assertTrue("Should be able to offer.", q.offer(0));
    assertEquals("Should see what I put in.", new Integer(0), q.peek());
    assertEquals("Should see get what put in.", new Integer(0), q.forget(q.poll()));
    assertNull("Should return null when empty", q.peek());
    q.close();
  }
  
  @Test
  public void poll() {
    ConstrainedQueue<Integer> q = smallQueue();
    assertTrue("Should be able to offer.", q.offer(0));
    assertEquals("Should see get what put in.", new Integer(0), q.forget(q.poll()));
    assertNull("Should return null when empty", q.poll());
    q.close();
  }
  
  @Test
  public void remove() {
    ConstrainedQueue<Integer> q = smallQueue();
    assertTrue("Should be able to offer.", q.offer(0));
    assertEquals("Should see get what put in.", new Integer(0), q.forget(q.remove()));
    expectException("Should have NoSuchElementException", NoSuchElementException.class, () -> { q.remove(); });
    q.close();
  }
  
  @Test
  public void addAll() {
    ConstrainedQueue<Integer> q = smallQueue();
    assertTrue("Should be able to addAll.", q.addAll(Arrays.asList(0, 1, 2)));
    for(int i = 0; i < 3; ++i) {
      assertEquals("Should see get what put in.", new Integer(i), q.forget(q.poll()));
    }
    assertNull("Should return null when empty", q.poll());
    q.close();
  }
  
  @Test
  public void clear() {
    ConstrainedQueue<Integer> q = smallQueue();
    assertTrue("Should be able to addAll.", q.addAll(Arrays.asList(0, 1, 2)));
    q.clear();
    assertTrue("Queue should be empty.", q.isEmpty());
    assertNull("Should return null when empty", q.poll());
    q.close();
  }
  
  @Test
  public void isEmpty() {
    ConstrainedQueue<Integer> q = smallQueue();
    assertTrue("Queue should be empty.", q.isEmpty());
    assertTrue("Should be able to addAll.", q.addAll(Arrays.asList(0, 1, 2)));
    q.clear();
    assertTrue("Queue should be empty.", q.isEmpty());
    assertNull("Should return null when empty", q.poll());
    q.close();    
  }
  
  @Test
  public void iterator() {
    ConstrainedQueue<Integer> q = smallQueue();
    assertTrue("Should be able to addAll.", q.addAll(Arrays.asList(0, 1, 2)));
    int i = 0;
    for(Integer j : q) {
      assertFalse("Shouldn't be empty", q.isEmpty());
      assertEquals("Should have the right number.", new Integer(i++), q.forget(j));
    }
    assertEquals("Should have iterated 3 times", new Integer(3), new Integer(i));
    assertTrue("Queue should be empty.", q.isEmpty());
    assertNull("Should return null when empty", q.poll());
    q.close();
  }
  
  @Test
  public void removeAll() {
    ConstrainedQueue<Integer> q = smallQueue();
    assertTrue("Queue should be empty.", q.isEmpty());
    assertTrue("Should be able to addAll.", q.addAll(Arrays.asList(0, 1, 2)));
    assertTrue("Should be able to removeAll.", q.removeAll(Arrays.asList(0, 1)));
    assertFalse("Should not be empty", q.isEmpty());
    assertEquals("Should still have 2.", new Integer(2), q.forget(q.poll()));
    assertTrue("Queue should be empty.", q.isEmpty());
    assertNull("Should return null when empty", q.poll());
    q.close();
  }
  
  @Test
  public void size() {
    ConstrainedQueue<Integer> q = smallQueue();
    assertTrue("Should be able to addAll.", q.addAll(Arrays.asList(0, 1, 2)));
    int i = 0;
    for(Integer j : q) {
      assertFalse("Shouldn't be empty", q.isEmpty());
      assertEquals("Should have the right number.", new Integer(i++), q.forget(j));
      assertEquals("Should be this size.", new Integer(3 - i), new Integer(q.size()));
    }
    assertEquals("Should have iterated 3 times", new Integer(3), new Integer(i));
    assertTrue("Queue should be empty.", q.isEmpty());
    assertNull("Should return null when empty", q.poll());
    q.close();
  } 
  
}
