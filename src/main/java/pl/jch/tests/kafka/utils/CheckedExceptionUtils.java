package pl.jch.tests.kafka.utils;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import lombok.experimental.UtilityClass;

@UtilityClass
public class CheckedExceptionUtils {
    public static <T, S> Function<T, S> wrapCheckedFunction(CheckedFunction<T, S> function) {
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

    public static <T> Consumer<T> wrapCheckedConsumer(CheckedConsumer<T> consumer) {
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

    public static <T> Supplier<T> wrapCheckedSupplier(CheckedSupplier<T> supplier) {
        return () -> {
            try {
                return supplier.get();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }


}
