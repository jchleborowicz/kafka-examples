package pl.jch.tests.kafka.utils;

import java.util.function.Consumer;
import java.util.function.Function;

import lombok.experimental.UtilityClass;

@UtilityClass
public class CheckedExceptionUtils {
    private static <T, S> Function<T, S> withoutCheckedException(
            FunctionWithCheckedException<T, S> function) {
        return param -> {
            try {
                return function.apply(param);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T> Consumer<T> withoutCheckedException(
            ConsumerWithCheckedException<T> consumer) {
        return param -> {
            try {
                consumer.accept(param);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    @FunctionalInterface
    public interface FunctionWithCheckedException<T, S> {
        S apply(T var1) throws Exception;
    }

    @FunctionalInterface
    public interface ConsumerWithCheckedException<T> {
        void accept(T var1) throws Exception;
    }
}
