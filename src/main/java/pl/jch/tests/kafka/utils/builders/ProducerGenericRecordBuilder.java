package pl.jch.tests.kafka.utils.builders;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import pl.jch.tests.kafka.utils.functions.CheckedFunction;

import static pl.jch.tests.kafka.utils.functions.CheckedExceptionUtils.wrapCheckedFunction;

public class ProducerGenericRecordBuilder<KeyT> {

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

    private static Schema createSchema(String fileName) {
        final String schemaString = readFile(fileName);

        final Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    private static String readFile(String fileName) {
        return wrapCheckedFunction((CheckedFunction<Path, String>) Files::readString)
                .apply(Paths.get(fileName));
    }

    public ProducerRecord<KeyT, GenericRecord> build() {
        final Schema schema = createSchema(this.schemaFile);
        final GenericRecord genericRecord = new GenericData.Record(schema);

        this.valueProperties.forEach(genericRecord::put);

        return new ProducerRecord<>(this.topic, this.key, genericRecord);
    }
}
