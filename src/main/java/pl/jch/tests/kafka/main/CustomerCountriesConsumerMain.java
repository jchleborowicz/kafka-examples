package pl.jch.tests.kafka.main;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import pl.jch.tests.kafka.utils.Topics;

import static pl.jch.tests.kafka.utils.CheckedExceptionUtils.wrapCheckedFunction;
import static pl.jch.tests.kafka.utils.KafkaBuilders.consumerBuilder;

public class CustomerCountriesConsumerMain {

    private static final String TOPIC = Topics.CUSTOMER_COUNTRIES;
    public static final Function<Object, String> JSON_SERIALIZER =
            wrapCheckedFunction(
                    new ObjectMapper().writerWithDefaultPrettyPrinter()::writeValueAsString
            );

    public static void main(String[] args) {
        consumerBuilder()
                .groupId("counter")
                .consumerDefinition()
                .topic(TOPIC)
                .pollDuration(Duration.ofSeconds(5))
                .onRecord(CustomerCountriesConsumerMain.recordHandler())
                .pollInfinitely();
    }

    private static Consumer<ConsumerRecord<String, String>> recordHandler() {
        final Map<String, Integer> custCountryMap = new HashMap<>();

        return record -> {
            final String country = record.value();
            System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s, country = %s%n",
                    record.topic(), record.partition(), record.offset(), record.key(), country);

            synchronized (custCountryMap) {
                final Integer currentCount = custCountryMap.getOrDefault(country, 1);
                custCountryMap.put(country, currentCount + 1);
            }

            prettyPrint(custCountryMap);
        };
    }

    private static void prettyPrint(Map<String, Integer> custCountryMap) {
        final String serialized = JSON_SERIALIZER.apply(custCountryMap);
        System.out.println(serialized);
    }

}
