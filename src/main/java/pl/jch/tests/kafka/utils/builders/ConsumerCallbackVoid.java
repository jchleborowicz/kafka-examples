package pl.jch.tests.kafka.utils.builders;

import org.apache.kafka.clients.consumer.Consumer;

public interface ConsumerCallbackVoid<KeyT, ValueT> {
    void execute(Consumer<KeyT, ValueT> consumer) throws Exception;
}
