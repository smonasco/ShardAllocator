package org.shannon.ConstrainedQueue.Test;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.shannon.ConstrainedQueue.ShardRelocationConstrainer;
import org.shannon.ShardAllocator.ShardRelocation;

public class ShardRelocationConstrainerTest {

  private ShardRelocationConstrainer<Integer, Integer> src(int size) {
    return new ShardRelocationConstrainer<Integer, Integer>(size);
  }
  
  private ShardRelocation<Integer, Integer> sr(Integer from, Integer to) {
    return new ShardRelocation<Integer, Integer>(from, to, 0);
  }
  
  @Test
  public void sameNodesTwiceConstrained() {
    ShardRelocationConstrainer<Integer, Integer> c = src(1);
    assertFalse("First entry shouldn't be constrained", c.constrained(sr(0, 1)));
    assertTrue("Repeat should be constrained", c.constrained(sr(0, 1)));
  }
  
  @Test
  public void oneNodeRepeatedConstrained() {
    ShardRelocationConstrainer<Integer, Integer> c = src(1);
    assertFalse("First entry shouldn't be constrained", c.constrained(sr(0, 1)));
    assertTrue("Repeat should be constrained", c.constrained(sr(0, 2)));
    c.clear();
    assertFalse("First entry shouldn't be constrained", c.constrained(sr(2, 1)));
    assertTrue("Repeat should be constrained", c.constrained(sr(1, 0)));
    c.clear();
    assertFalse("First entry shouldn't be constrained", c.constrained(sr(0, 1)));
    assertTrue("Repeat should be constrained", c.constrained(sr(2, 1)));
    c.clear();
    assertFalse("First entry shouldn't be constrained", c.constrained(sr(0, 1)));
    assertFalse("Non-repeat shouldn't be constrained", c.constrained(sr(2, 4)));
  }
  
  @Test
  public void constrained2() {
    ShardRelocationConstrainer<Integer, Integer> c = src(1);
    assertFalse("First entry shouldn't be constrained", c.constrained(sr(0, 1), 1, TimeUnit.MILLISECONDS));
    assertTrue("Repeat should be constrained", c.constrained(sr(0, 2), 1, TimeUnit.MILLISECONDS));
    c.clear();
    assertFalse("First entry shouldn't be constrained", c.constrained(sr(2, 1), 1, TimeUnit.MILLISECONDS));
    assertTrue("Repeat should be constrained", c.constrained(sr(1, 0), 1, TimeUnit.MILLISECONDS));
    c.clear();
    assertFalse("First entry shouldn't be constrained", c.constrained(sr(0, 1), 1, TimeUnit.MILLISECONDS));
    assertTrue("Repeat should be constrained", c.constrained(sr(2, 1), 1, TimeUnit.MILLISECONDS));
    c.clear();
    assertFalse("First entry shouldn't be constrained", c.constrained(sr(0, 1), 1, TimeUnit.MILLISECONDS));
    assertFalse("Non-repeat shouldn't be constrained", c.constrained(sr(2, 4), 1, TimeUnit.MILLISECONDS));
  }
  
  @Test
  public void notifyReleased() {
    //Something simple
    ShardRelocationConstrainer<Integer, Integer> c = src(1);
    assertFalse("First entry shouldn't be constrained", c.constrained(sr(0, 1)));
    assertTrue("Repeat should be constrained", c.constrained(sr(0, 2)));
    assertTrue("Repeat should be constrained", c.constrained(sr(0, 2)));
    assertFalse("First entry shouldn't be constrained", c.constrained(sr(3, 4)));
    assertTrue("Repeat should be constrained", c.constrained(sr(3, 4)));
    Collection<ShardRelocation<Integer, Integer>> released = c.notifyReleased(sr(0, 1));
    assertEquals("Should release so many", new Integer(1), new Integer(released.size()));
    for(ShardRelocation<Integer, Integer> sr : released) {
      assertEquals("Should release the one I expect", sr(0, 2), sr);
    }
    released = c.notifyReleased(sr(3, 4));
    assertEquals("Should release so many", new Integer(1), new Integer(released.size()));
    for(ShardRelocation<Integer, Integer> sr : released) {
      assertEquals("Should release the one I expect", sr(3, 4), sr);
    }
    
    //more than 1 released
    c.clear();
    assertFalse("First entry shouldn't be constrained", c.constrained(sr(0, 1)));
    assertTrue("Repeat should be constrained", c.constrained(sr(0, 2)));
    assertTrue("Repeat should be constrained", c.constrained(sr(1, 3)));
    assertFalse("Constrained entries don't constrain others", c.constrained(sr(3, 2)));
    released = c.notifyReleased(sr(0, 1));
    released = c.notifyReleased(sr(3, 2));
    assertEquals("Should release so many", new Integer(2), new Integer(released.size()));
    assertTrue("Should release the one I expect", released.remove(sr(0, 2)));
    assertTrue("Should release the one I expect", released.remove(sr(1, 3)));
    
    //a depends on b and c.  b gets released, but c is still out
    c.clear();
    assertFalse("First entry shouldn't be constrained", c.constrained(sr(0, 1)));
    assertFalse("First entry shouldn't be constrained", c.constrained(sr(4, 2)));
    assertTrue("Repeat should be constrained", c.constrained(sr(0, 2)));
    released = c.notifyReleased(sr(0, 1));
    assertEquals("Should release none", new Integer(0), new Integer(released.size()));
    released = c.notifyReleased(sr(4, 2));
    assertEquals("Should release so many", new Integer(1), new Integer(released.size()));
    assertTrue("Should release the one I expect", released.remove(sr(0, 2)));
  }
}
