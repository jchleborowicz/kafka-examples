package pl.jch.tests.kafka.utils;

import java.time.Duration;
import java.util.function.Consumer;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import static java.util.Collections.singletonList;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PollingConsumerCallbackBuilder<KeyT, ValueT> {

    private String topic;
    private Duration pollDuration;
    private java.util.function.Consumer<ConsumerRecord<KeyT, ValueT>> recordConsumer;

    public static <KeyT, ValueT> PollingConsumerCallbackBuilder<KeyT, ValueT> builder() {
        return new PollingConsumerCallbackBuilder<>();
    }

    public PollingConsumerCallbackBuilder<KeyT, ValueT> topic(String topic) {
        this.topic = topic;
        return this;
    }

    public PollingConsumerCallbackBuilder<KeyT, ValueT> pollDuration(Duration duration) {
        this.pollDuration = duration;
        return this;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <S, T> PollingConsumerCallbackBuilder<S, T> onRecord(Consumer<ConsumerRecord<S, T>> recordConsumer) {
        this.recordConsumer = (Consumer) recordConsumer;
        return (PollingConsumerCallbackBuilder) this;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public ConsumerCallbackVoid<KeyT, ValueT> build() {
        return consumer -> {
            consumer.subscribe(singletonList(PollingConsumerCallbackBuilder.this.topic));

            while (true) {
                final ConsumerRecords<KeyT, ValueT> records =
                        consumer.poll(PollingConsumerCallbackBuilder.this.pollDuration);

                for (ConsumerRecord<KeyT, ValueT> record : records) {
                    PollingConsumerCallbackBuilder.this.recordConsumer.accept(record);
                }
            }
        };
    }
}
