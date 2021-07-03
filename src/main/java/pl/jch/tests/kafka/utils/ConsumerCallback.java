package pl.jch.tests.kafka.utils;

import org.apache.kafka.clients.consumer.Consumer;

public interface ConsumerCallback<KeyT, ValueT, ResultT> {
    ResultT execute(Consumer<KeyT, ValueT> producer) throws Exception;
}
