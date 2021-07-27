package pl.jch.tests.kafka.main.avro;

import java.util.concurrent.Future;
import java.util.stream.IntStream;

import io.confluent.examples.clients.basicavro.Payment;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import pl.jch.tests.kafka.utils.Topics;

import static pl.jch.tests.kafka.utils.builders.KafkaBuilders.producerBuilder;
import static pl.jch.tests.kafka.utils.functions.CheckedExceptionUtils.wrapCheckedFunction;

public class AvroTransactionsProducerMain {

    private static final String TOPIC = Topics.TRANSACTIONS;

    public static void main(final String[] args) {
        producerBuilder(StringSerializer.class, KafkaAvroSerializer.class)
                .buildAndExecute(AvroTransactionsProducerMain::execute);
    }

    private static void execute(Producer<String, Payment> producer) {
        IntStream.range(0, 10)
                .mapToObj(i -> "id" + i)
                .map(orderId -> new Payment(orderId, 1000.00d))
                .map(payment -> new ProducerRecord<>(TOPIC, payment.getId().toString(), payment))
                .map(producer::send)
                .map(wrapCheckedFunction(Future::get))
                .forEach(System.out::println);

        producer.flush();
        System.out.printf("Successfully produced 10 messages to a topic called %s%n", TOPIC);
    }

}
