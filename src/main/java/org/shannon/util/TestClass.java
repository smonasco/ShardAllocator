package org.shannon.util;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.shannon.function.ExceptionalActor;

public class TestClass {
  private ArrayList<CompletableFuture<Void>> delayedExceptions = new ArrayList<CompletableFuture<Void>>();
  
  protected <T extends Throwable> void expectException(String message, Class<T> type, ExceptionalActor<Throwable> actor) {
    Throwable t = null;
    try {
      actor.act();
    } catch (Throwable e) {
      if (e.getClass().isAssignableFrom(type)) {
        t = e;
      } else {
        System.out.println("Excepted " + type + " but got " + e.toString());
      }
    }
    assertNotNull(message, t);
  }
  
  protected void captureException(ExceptionalActor<Throwable> actor){
    CompletableFuture<Void> f = new CompletableFuture<Void>();
    try {
      actor.act();
      f.complete(null);
    } catch (Throwable e) {
      f.completeExceptionally(e);
    }
    delayedExceptions.add(f);
  }
  
  @After
  public void releaseExceptions() throws Throwable {
    try {
      CompletableFuture.allOf(delayedExceptions.toArray(new CompletableFuture[delayedExceptions.size()])).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }
}
