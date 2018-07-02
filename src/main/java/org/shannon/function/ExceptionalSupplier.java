package org.shannon.function;

@FunctionalInterface
public interface ExceptionalSupplier<E extends Throwable, T> {
  T get() throws E;
}
