package pl.jch.tests.kafka.utils.functions;

@FunctionalInterface
public interface CheckedFunction<T, S> {
    S apply(T var1) throws Exception;
}
