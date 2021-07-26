package pl.jch.tests.kafka.utils.functions;

@FunctionalInterface
public interface CheckedConsumer<T> {
    void accept(T var1) throws Exception;
}
