package org.shannon.function;

@FunctionalInterface
public interface ExceptionalSupplier<E extends Throwable, T> {
  public T get() throws E;
}
