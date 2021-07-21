package pl.jch.tests.kafka.main;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import pl.jch.tests.kafka.utils.Topics;

import static pl.jch.tests.kafka.utils.KafkaBuilders.consumerBuilder;
import static pl.jch.tests.kafka.utils.LoggingUtils.prettyPrint;

public class CustomerCountriesConsumerMain {

    private static final String TOPIC = Topics.CUSTOMER_COUNTRIES;

    public static void main(String[] args) {
        consumerBuilder()
                .groupId("counter")
                .defineIncomingRecordsHandler()
                .topic(TOPIC)
                .pollDuration(Duration.ofSeconds(5))
                .onRecord(CustomerCountriesConsumerMain.recordHandler())
                .pollInfinitely();
    }

    private static Consumer<ConsumerRecord<String, String>> recordHandler() {
        final Map<String, Integer> customerCountByCountry = new HashMap<>();

        return record -> {
            final String country = record.value();
            System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s, country = %s%n",
                    record.topic(), record.partition(), record.offset(), record.key(), country);

            synchronized (customerCountByCountry) {
                final Integer currentCount = customerCountByCountry.getOrDefault(country, 0);
                customerCountByCountry.put(country, currentCount + 1);
            }

            prettyPrint(customerCountByCountry);
        };
    }

}
