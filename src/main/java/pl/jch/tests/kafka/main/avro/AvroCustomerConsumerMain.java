package pl.jch.tests.kafka.main.avro;

import java.time.Duration;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import pl.jch.tests.kafka.utils.Topics;
import pl.jch.tests.kafka.utils.kafka.AutoOffsetReset;

import static pl.jch.tests.kafka.utils.builders.KafkaBuilders.consumerBuilder;
import static pl.jch.tests.kafka.utils.builders.KafkaBuilders.pollingConsumerCallbackBuilder;

public class AvroCustomerConsumerMain {

    public static final String TOPIC = Topics.CUSTOMERS;

    public static void main(String[] args) {
        consumerBuilder(StringDeserializer.class, KafkaAvroDeserializer.class)
                .groupId("test1")
                .autoOffsetReset(AutoOffsetReset.EARLIEST)
                .enableAutoCommit(false)
                .execute(
                        pollingConsumerCallbackBuilder()
                                .topic(TOPIC)
                                .pollDuration(Duration.ofSeconds(5))
                                .onRecord(System.out::println)
                                .build()
                );
    }

}
