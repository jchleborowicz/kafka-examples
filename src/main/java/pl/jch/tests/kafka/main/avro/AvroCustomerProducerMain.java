package pl.jch.tests.kafka.main.avro;

import java.util.concurrent.ExecutionException;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import pl.jch.tests.kafka.utils.Topics;
import pl.jch.tests.kafka.utils.builders.ProducerGenericRecordBuilder;

import static pl.jch.tests.kafka.utils.builders.KafkaBuilders.producerBuilder;

public class AvroCustomerProducerMain {

    public static final String TOPIC = Topics.CUSTOMERS;

    public static void main(String[] args) {
        producerBuilder(StringSerializer.class, KafkaAvroSerializer.class)
                .execute(AvroCustomerProducerMain::execute);
    }

    private static void execute(Producer<String, GenericRecord> producer)
            throws ExecutionException, InterruptedException {
        final ProducerRecord<String, GenericRecord> record = ProducerGenericRecordBuilder.<String>builder()
                .topic(TOPIC)
                .schemaFile("src/main/resources/avro/Customer-v2.avsc")
                .key("Brajan")
                .valueProperty("id", 1)
                .valueProperty("name", "Brajan")
                .valueProperty("email", "brajan@seba.com.pl")
                .build();

        producer.send(record)
                .get();
    }

}
