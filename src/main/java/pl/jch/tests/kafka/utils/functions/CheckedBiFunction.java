package pl.jch.tests.kafka.utils.functions;

public interface CheckedBiFunction<T, U, R> {
    R apply(T var1, U var2) throws Exception;
}
