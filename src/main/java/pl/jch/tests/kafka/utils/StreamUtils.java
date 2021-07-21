package pl.jch.tests.kafka.utils;

import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class StreamUtils {

    public static <T> Stream<T> iteratorToStream(Iterator<T> iterator) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
                false
        );
    }

    public static <T> Stream<T> factoryMethodToStream(Supplier<Optional<T>> factoryMethod) {
        return iteratorToStream(optionalSupplierToIterator(factoryMethod));
    }

    private static <T> Iterator<T> optionalSupplierToIterator(Supplier<Optional<T>> factoryMethod) {
        return new Iterator<>() {

            private T next;
            private boolean hasNext = true;

            {
                this.next();
            }

            @Override
            public boolean hasNext() {
                return this.hasNext;
            }

            @Override
            public T next() {
                if (!this.hasNext) {
                    throw new RuntimeException("No more values");
                }
                final T current = this.next;

                final Optional<T> nextOptional = factoryMethod.get();
                this.hasNext = nextOptional.isPresent();
                this.next = nextOptional.orElse(null);

                return current;
            }
        };
    }

}
