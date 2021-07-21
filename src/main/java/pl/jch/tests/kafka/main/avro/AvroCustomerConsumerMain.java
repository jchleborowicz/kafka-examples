package pl.jch.tests.kafka.main.avro;

import java.time.Duration;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import pl.jch.tests.kafka.utils.AutoOffsetReset;
import pl.jch.tests.kafka.utils.ConsumerBuilder;
import pl.jch.tests.kafka.utils.PollingConsumerBuilder;
import pl.jch.tests.kafka.utils.Topics;

public class AvroCustomerConsumerMain {

    public static final String TOPIC = Topics.CUSTOMERS;

    public static void main(String[] args) {
        ConsumerBuilder.builder(StringDeserializer.class, KafkaAvroDeserializer.class)
                .groupId("test1")
                .autoOffsetReset(AutoOffsetReset.EARLIEST)
                .enableAutoCommit(false)
                .execute(
                        PollingConsumerBuilder.builder()
                                .topic(TOPIC)
                                .pollDuration(Duration.ofSeconds(5))
                                .onRecord(System.out::println)
                );
    }

}
