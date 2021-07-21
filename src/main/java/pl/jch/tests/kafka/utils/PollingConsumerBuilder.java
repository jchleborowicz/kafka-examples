package pl.jch.tests.kafka.utils;

import java.time.Duration;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import pl.jch.tests.kafka.main.avro.AvroCustomerConsumerMain;

import static java.util.Collections.singletonList;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PollingConsumerBuilder {

    private String topic;
    private Duration pollDuration;

    public static PollingConsumerBuilder builder() {
        return new PollingConsumerBuilder();
    }

    public PollingConsumerBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    public PollingConsumerBuilder pollDuration(Duration duration) {
        this.pollDuration = duration;
        return this;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public <KeyT, ValueT> ConsumerCallbackVoid<KeyT, ValueT> onRecord(
            java.util.function.Consumer<ConsumerRecord<KeyT, ValueT>> recordConsumer) {
        return consumer -> {
            consumer.subscribe(singletonList(PollingConsumerBuilder.this.topic));

            while (true) {
                final ConsumerRecords<KeyT, ValueT> records =
                        consumer.poll(PollingConsumerBuilder.this.pollDuration);

                for (ConsumerRecord<KeyT, ValueT> record : records) {
                    recordConsumer.accept(record);
                }
            }
        };
    }
}
