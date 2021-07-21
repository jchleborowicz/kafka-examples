package pl.jch.tests.kafka.utils;

@FunctionalInterface
public interface CheckedFunction<T, S> {
    S apply(T var1) throws Exception;
}
