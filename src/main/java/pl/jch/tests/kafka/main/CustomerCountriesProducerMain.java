package pl.jch.tests.kafka.main;

import java.io.BufferedReader;
import java.io.Console;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pl.jch.tests.kafka.utils.ProducerCallbackVoid;
import pl.jch.tests.kafka.utils.Topics;

import static pl.jch.tests.kafka.utils.KafkaBuilders.consumerBuilder;
import static pl.jch.tests.kafka.utils.KafkaBuilders.producerBuilder;
import static pl.jch.tests.kafka.utils.LoggingUtils.prettyPrint;

public class CustomerCountriesProducerMain {

    private static final String TOPIC = Topics.CUSTOMER_COUNTRIES;

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) {
        producerBuilder()
                .execute((ProducerCallbackVoid<String, String>) producer -> {
                    while (true) {
                        final String s = new BufferedReader(new InputStreamReader(System.in)).readLine();
                        final String[] split = s.split(" ");
                        if (split.length == 2) {
                            producer.send(new ProducerRecord<>(TOPIC, split[0], split[1]));
                        }
                    }

                });
    }

    private static Consumer<ConsumerRecord<String, String>> recordHandler() {
        final Map<String, Integer> customerCountByCountry = new HashMap<>();

        return record -> {
            final String country = record.value();
            System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s, country = %s%n",
                    record.topic(), record.partition(), record.offset(), record.key(), country);

            synchronized (customerCountByCountry) {
                final Integer currentCount = customerCountByCountry.getOrDefault(country, 1);
                customerCountByCountry.put(country, currentCount + 1);
            }

            prettyPrint(customerCountByCountry);
        };
    }

}
