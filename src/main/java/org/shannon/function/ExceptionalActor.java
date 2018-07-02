package org.shannon.function;

@FunctionalInterface
public interface ExceptionalActor<T extends Throwable> {
  void act() throws T;
}
