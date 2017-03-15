package org.shannon.util;

import static org.junit.Assert.assertNotNull;

import org.shannon.function.ExceptionalActor;

public class JUnitHelper {
  
  public static <T extends Throwable> void expectException(String message, Class<T> type, ExceptionalActor<Throwable> actor) {
    Throwable t = null;
    try {
      actor.act();
    } catch (Throwable e) {
      if (e.getClass() == type) {
        t = e;
      } else {
        System.out.println("Excepted " + type + " but got " + e.toString());
      }
    }
    assertNotNull(message, t);
  }
}
