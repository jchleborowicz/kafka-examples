package pl.jch.tests.kafka.utils.functions;

@FunctionalInterface
public interface CheckedSupplier<T> {
    T get() throws Exception;
}
