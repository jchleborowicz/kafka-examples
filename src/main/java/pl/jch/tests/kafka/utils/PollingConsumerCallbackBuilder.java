package pl.jch.tests.kafka.utils;

import java.time.Duration;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class PollingConsumerCallbackBuilder<KeyT, ValueT> {

    private final Consumer<ConsumerCallbackVoid<KeyT, ValueT>> executeCallback;
    private String topic;
    private Duration pollDuration;
    private java.util.function.Consumer<ConsumerRecord<KeyT, ValueT>> recordConsumer;

    public PollingConsumerCallbackBuilder() {
        this(null);
    }

    private PollingConsumerCallbackBuilder(Consumer<ConsumerCallbackVoid<KeyT, ValueT>> executeCallback) {
        this.executeCallback = executeCallback;
    }

    public static <KeyT, ValueT> PollingConsumerCallbackBuilder<KeyT, ValueT> builder() {
        return new PollingConsumerCallbackBuilder<>();
    }

    public static <KeyT, ValueT> PollingConsumerCallbackBuilder<KeyT, ValueT> builder(
            Consumer<ConsumerCallbackVoid<KeyT, ValueT>> executeCallback) {
        return new PollingConsumerCallbackBuilder<>(executeCallback);
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
        requireNonNull(this.pollDuration);
        requireNonNull(this.topic);
        requireNonNull(this.recordConsumer);

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

    public void pollInfinitely() {
        if (this.executeCallback == null) {
            throw new RuntimeException("Cannot poll without execute callback");
        }

        final ConsumerCallbackVoid<KeyT, ValueT> consumerCallback = this.build();

        this.executeCallback.accept(consumerCallback);
    }
}
