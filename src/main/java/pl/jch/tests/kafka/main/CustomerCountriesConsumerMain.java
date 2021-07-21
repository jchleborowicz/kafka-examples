package pl.jch.tests.kafka.main;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import pl.jch.tests.kafka.utils.KafkaBuilders;
import pl.jch.tests.kafka.utils.Topics;

import static pl.jch.tests.kafka.utils.KafkaBuilders.consumerBuilder;
import static pl.jch.tests.kafka.utils.KafkaBuilders.pollingConsumerCallbackBuilder;

public class CustomerCountriesConsumerMain {

    private static final String TOPIC = Topics.CUSTOMER_COUNTRIES;

    public static void main(String[] args) {
        consumerBuilder()
                .execute(
                        pollingConsumerCallbackBuilder()
                                .topic(TOPIC)
                                .onRecord(CustomerCountriesConsumerMain::onRecord)
                                .build()
                );
    }

    private static void onRecord(ConsumerRecord<String, String> objectObjectConsumerRecord) {

    }
}
