package org.shannon.function;

@FunctionalInterface
public interface ExceptionalActor<T extends Throwable> {
  public void act() throws T;
}
