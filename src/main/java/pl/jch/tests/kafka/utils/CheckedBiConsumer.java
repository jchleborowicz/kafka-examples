package pl.jch.tests.kafka.utils;

@FunctionalInterface
public interface CheckedBiConsumer<T, S> {
    void accept(T var1, S var2) throws Exception;
}
