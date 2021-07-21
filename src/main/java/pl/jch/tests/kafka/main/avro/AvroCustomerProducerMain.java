package pl.jch.tests.kafka.main.avro;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import pl.jch.tests.kafka.utils.Topics;

import static pl.jch.tests.kafka.utils.KafkaBuilders.producerBuilder;

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

    public static class ProducerGenericRecordBuilder<KeyT> {
        private String topic;
        private String schemaFile;
        private KeyT key;
        private final Map<String, Object> valueProperties = new HashMap<>();

        public static <T> ProducerGenericRecordBuilder<T> builder() {
            return new ProducerGenericRecordBuilder<>();
        }

        public ProducerGenericRecordBuilder<KeyT> topic(String topic) {
            this.topic = topic;
            return this;
        }

        public ProducerGenericRecordBuilder<KeyT> schemaFile(String schemaFile) {
            this.schemaFile = schemaFile;
            return this;
        }

        public ProducerGenericRecordBuilder<KeyT> valueProperty(String key, Object value) {
            this.valueProperties.put(key, value);
            return this;
        }

        public ProducerGenericRecordBuilder<KeyT> key(KeyT key) {
            this.key = key;
            return this;
        }

        public ProducerRecord<KeyT, GenericRecord> build() {
            final Schema schema = createSchema(this.schemaFile);
            final GenericRecord genericRecord = new GenericData.Record(schema);

            this.valueProperties.forEach(genericRecord::put);

            return new ProducerRecord<>(this.topic, this.key, genericRecord);
        }
    }

    private static Schema createSchema(String fileName) {
        final String schemaString = readFile(fileName);

        final Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    private static String readFile(String fileName) {
        try {
            return Files.readString(Paths.get(fileName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
