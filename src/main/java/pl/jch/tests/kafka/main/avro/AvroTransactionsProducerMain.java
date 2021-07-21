package pl.jch.tests.kafka.main.avro;

import io.confluent.examples.clients.basicavro.Payment;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import pl.jch.tests.kafka.utils.Topics;

import static pl.jch.tests.kafka.utils.KafkaBuilders.producerBuilder;

public class AvroTransactionsProducerMain {

    private static final String TOPIC = Topics.TRANSACTIONS;

    public static void main(final String[] args) {
        producerBuilder(StringSerializer.class, KafkaAvroSerializer.class)
                .execute(AvroTransactionsProducerMain::execute);
    }

    private static void execute(Producer<String, Payment> producer) throws InterruptedException {
        for (long i = 0; i < 10; i++) {
            final String orderId = "id" + i;
            final Payment payment = new Payment(orderId, 1000.00d);
            final ProducerRecord<String, Payment> record =
                    new ProducerRecord<>(TOPIC, payment.getId().toString(), payment);
            producer.send(record);
            Thread.sleep(1L);
        }

        producer.flush();
        System.out.printf("Successfully produced 10 messages to a topic called %s%n", TOPIC);
    }
}
