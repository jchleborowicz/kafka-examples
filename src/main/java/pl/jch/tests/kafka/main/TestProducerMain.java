package pl.jch.tests.kafka.main;

import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pl.jch.tests.kafka.utils.ProducerBuilder;
import pl.jch.tests.kafka.utils.Topics;

public class TestProducerMain {

    public static final String TOPIC = Topics.TEST;

    public static void main(String[] args) {
        ProducerBuilder.builder()
                .execute(TestProducerMain::execute);
    }

    private static void execute(Producer<String, String> producer) {
        final ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "Precision Products", "Fraeounce");

        final Instant start = Instant.now();
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                throw new RuntimeException(exception);
            }
            final Instant now = Instant.now();
            System.out.println("After receive: " + Duration.between(start, now));
            System.out.println("Received " + metadata);
        });
        final Instant stop = Instant.now();

        System.out.println("After send: " + Duration.between(start, stop));
    }
}
