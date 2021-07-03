package pl.jch.tests.kafka.utils;

import org.apache.kafka.clients.producer.Producer;

public interface ProducerCallbackVoid<KeyT, ValueT> {
    void execute(Producer<KeyT, ValueT> producer) throws Exception;
}
