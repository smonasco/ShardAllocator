package org.shannon.ShardAllocator.test;

import org.junit.Test;
import org.shannon.ShardAllocator.Impl.SimpleAllocator;

import static org.shannon.util.JUnitHelper.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SimpleAllocatorTest {

  @Test
  public void constructorValidation() {
    expectException("Nodes cannot be null", IllegalArgumentException.class
        , () -> { 
          new SimpleAllocator<Integer, Integer>((Collection<Integer>)null, Arrays.asList(0), (Map<Integer, Collection<Integer>>)null
            , () -> { return new HashMap<Integer, Collection<Integer>>(); }, (move) -> { }, 1); 
      });
    expectException("Nodes cannot be empty", IllegalArgumentException.class
        , () -> { 
          new SimpleAllocator<Integer, Integer>(new ArrayList<Integer>(), Arrays.asList(0), (Map<Integer, Collection<Integer>>)null
            , () -> { return new HashMap<Integer, Collection<Integer>>(); }, (move) -> { }, 1); 
      });
    expectException("Shards cannot be null", IllegalArgumentException.class
        , () -> { 
          new SimpleAllocator<Integer, Integer>(Arrays.asList(0), (Collection<Integer>)null, (Map<Integer, Collection<Integer>>)null
            , () -> { return new HashMap<Integer, Collection<Integer>>(); }, (move) -> { }, 1); 
      });
    expectException("Shards cannot be empty", IllegalArgumentException.class
        , () -> { 
          new SimpleAllocator<Integer, Integer>(Arrays.asList(0), new ArrayList<Integer>(), (Map<Integer, Collection<Integer>>)null
            , () -> { return new HashMap<Integer, Collection<Integer>>(); }, (move) -> { }, 1); 
      });
  }
  
}
