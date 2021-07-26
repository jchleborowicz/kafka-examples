package pl.jch.tests.kafka.utils.functions;

@FunctionalInterface
public interface CheckedBiConsumer<T, S> {
    void accept(T var1, S var2) throws Exception;
}
