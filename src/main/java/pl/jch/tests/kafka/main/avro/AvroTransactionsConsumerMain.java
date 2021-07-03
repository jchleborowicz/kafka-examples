package pl.jch.tests.kafka.main.avro;

import java.time.Duration;

import io.confluent.examples.clients.basicavro.Payment;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import pl.jch.tests.kafka.utils.ConsumerBuilder;
import pl.jch.tests.kafka.utils.Topics;

import static java.util.Collections.singletonList;

public class AvroTransactionsConsumerMain {

    private static final String TOPIC = Topics.TRANSACTIONS;

    public static void main(final String[] args) {
        ConsumerBuilder.builder(StringDeserializer.class, KafkaAvroDeserializer.class)
                .groupId("test-payments")
                .config(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)
                .execute(AvroTransactionsConsumerMain::execute);
    }

    @SuppressWarnings("InfiniteLoopStatement")
    private static Void execute(Consumer<String, Payment> consumer) {
        consumer.subscribe(singletonList(TOPIC));

        while (true) {
            final ConsumerRecords<String, Payment> records = consumer.poll(Duration.ofMillis(100));
            for (final ConsumerRecord<String, Payment> record : records) {
                final String key = record.key();
                final Payment value = record.value();
                System.out.printf("key = %s, value = %s%n", key, value);
            }
        }
    }
}
