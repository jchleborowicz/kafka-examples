package pl.jch.tests.kafka.utils;

import org.apache.kafka.clients.consumer.Consumer;

public interface ConsumerCallbackVoid<KeyT, ValueT> {
    void execute(Consumer<KeyT, ValueT> producer) throws Exception;
}
