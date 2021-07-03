package pl.jch.tests.kafka.utils;

import org.apache.kafka.clients.producer.Producer;

public interface ProducerCallback<KeyT, ValueT, ResultT> {
    ResultT execute(Producer<KeyT, ValueT> producer) throws Exception;
}
