package pl.jch.tests.kafka.main.avro;

import java.time.Duration;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import pl.jch.tests.kafka.utils.ConsumerBuilder;
import pl.jch.tests.kafka.utils.Topics;

import static java.util.Collections.singletonList;

public class AvroCustomerConsumerMain {

    public static final String TOPIC = Topics.CUSTOMERS;

    public static void main(String[] args) {
        ConsumerBuilder.builder(StringDeserializer.class, KafkaAvroDeserializer.class)
                .groupId("test")
                .enableAutoCommit(false)
                .execute(AvroCustomerConsumerMain::execute);
    }

    @SuppressWarnings("InfiniteLoopStatement")
    private static void execute(Consumer<String, GenericRecord> consumer) {
        consumer.subscribe(singletonList(TOPIC));

        while (true) {
            final ConsumerRecords<String, GenericRecord> poll = consumer.poll(Duration.ofSeconds(5));

            for (ConsumerRecord<String, GenericRecord> record : poll) {
                System.out.println("**** NEW RECORD");

                System.out.println(record.value());
            }
        }
    }
}
