package pl.jch.tests.kafka.main.avro;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import pl.jch.tests.kafka.utils.ProducerBuilder;
import pl.jch.tests.kafka.utils.Topics;

public class AvroCustomerProducerMain {

    public static final String TOPIC = Topics.CUSTOMERS;

    public static void main(String[] args) {
        ProducerBuilder.builder(StringSerializer.class, KafkaAvroSerializer.class)
                .execute(AvroCustomerProducerMain::execute);
    }

    private static void execute(Producer<String, GenericRecord> producer)
            throws IOException, ExecutionException, InterruptedException {
        final String schemaString = Files.readString(Paths.get("src/main/resources/avro/Customer-v2.avsc"));

        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(schemaString);
        final GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("id", 1);
        genericRecord.put("name", "Brajan");
        genericRecord.put("email", "brajan@seba.com.pl");

        final ProducerRecord<String, GenericRecord> record =
                new ProducerRecord<>(TOPIC, "Brajan", genericRecord);
        producer.send(record)
            .get();
    }
}
