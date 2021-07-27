package pl.jch.tests.kafka.main;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import pl.jch.tests.kafka.utils.KeyValue;
import pl.jch.tests.kafka.utils.StdIn;
import pl.jch.tests.kafka.utils.Topics;

import static pl.jch.tests.kafka.utils.builders.KafkaBuilders.producerBuilder;

public class CustomerCountriesProducerMain {

    private static final String TOPIC = Topics.CUSTOMER_COUNTRIES;

    public static void main(String[] args) {
        producerBuilder()
                .defineOutgoingRecordsProducer()
                .topic(TOPIC)
                .keyValuesSupplier(CustomerCountriesProducerMain::createKeyValue)
                .sendRecords();
    }

    private static Optional<KeyValue<String, String>> createKeyValue() {
        return Stream.generate(StdIn::readLine)
                .map(CustomerCountriesProducerMain::parseKeyValue)
                .filter(Objects::nonNull)
                .findFirst();
    }

    private static KeyValue<String, String> parseKeyValue(String line) {
        final String[] split = line.trim().split(" ");
        if (split.length == 2) {
            return KeyValue.of(split[0], split[1]);
        }
        return null;
    }

}
