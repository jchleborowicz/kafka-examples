package pl.jch.tests.kafka.utils.builders;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.experimental.UtilityClass;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;

@UtilityClass
public class KafkaBuilders {

    public static ConsumerBuilder consumerBuilder() {
        return ConsumerBuilder.builder();
    }

    public static ConsumerBuilder consumerBuilder(Class<StringDeserializer> keyDeserializer,
                                                  Class<KafkaAvroDeserializer> valueDeserializer) {
        return ConsumerBuilder.builder(keyDeserializer, valueDeserializer);
    }

    public static ProducerBuilder producerBuilder() {
        return ProducerBuilder.builder();
    }

    public static ProducerBuilder producerBuilder(Class<? extends Serializer<?>> keySerializerClass,
                                                  Class<? extends Serializer<?>> valueSerializerClass) {
        return ProducerBuilder.builder()
                .keySerializer(keySerializerClass)
                .valueSerializer(valueSerializerClass);
    }

    public static <KeyT, ValueT> PollingConsumerCallbackBuilder<KeyT, ValueT> pollingConsumerCallbackBuilder() {
        return PollingConsumerCallbackBuilder.builder();
    }

    public static AdminClientBuilder adminClientBuilder() {
        return AdminClientBuilder.builder();
    }

}
