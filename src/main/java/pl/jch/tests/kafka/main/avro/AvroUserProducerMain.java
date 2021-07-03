package pl.jch.tests.kafka.main.avro;

import java.util.concurrent.ExecutionException;

import example.avro.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import pl.jch.tests.kafka.utils.ProducerBuilder;
import pl.jch.tests.kafka.utils.Topics;

public class AvroUserProducerMain {

    public static final String TOPIC = Topics.USERS;

    public static void main(String[] args) {
        System.out.println(
                ProducerBuilder.builder(StringSerializer.class, KafkaAvroSerializer.class)
                        .execute(AvroUserProducerMain::execute)
        );
    }

    private static RecordMetadata execute(Producer<CharSequence, User> producer)
            throws ExecutionException, InterruptedException {
        final User user = User.newBuilder()
                .setName("Jerzy")
                .setFavoriteNumber(12)
                .setFavoriteColor("blue")
                .build();

        final ProducerRecord<CharSequence, User> record = new ProducerRecord<>(TOPIC, user.getName(), user);

        return producer.send(record).get();
    }
}
