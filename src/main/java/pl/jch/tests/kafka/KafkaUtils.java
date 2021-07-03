package pl.jch.tests.kafka;

import java.util.Properties;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class KafkaUtils {

    static KafkaProducer<String, String> createProducer() {
        final Properties kafkaProperties = createKafkaProperties();

        return new KafkaProducer<>(kafkaProperties);
    }

    private static Properties createKafkaProperties() {
        final Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9093");
        kafkaProperties.put("key.serializer", StringSerializer.class.getName());
        kafkaProperties.put("value.serializer", StringSerializer.class.getName());
        return kafkaProperties;
    }
}
