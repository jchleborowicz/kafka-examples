package pl.jch.tests.kafka.utils;

@FunctionalInterface
public interface CheckedSupplier<T> {
    T get() throws Exception;
}
