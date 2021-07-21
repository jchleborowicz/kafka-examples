package pl.jch.tests.kafka.utils;

import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.ProducerRecord;

import static pl.jch.tests.kafka.utils.CheckedExceptionUtils.wrapCheckedFunction;

public class OutgoingRecordsProducerBuilder<KeyT, ValueT> {

    private final Consumer<ProducerCallbackVoid<KeyT, ValueT>> executeCallback;
    private String topic;
    private Stream<ProducerRecord<KeyT, ValueT>> recordSupplier;

    public <S, T> OutgoingRecordsProducerBuilder(Consumer<ProducerCallbackVoid<KeyT, ValueT>> executeCallback) {
        this.executeCallback = executeCallback;
    }

    public static <KeyT, ValueT> OutgoingRecordsProducerBuilder<KeyT, ValueT> builder(
            Consumer<ProducerCallbackVoid<KeyT, ValueT>> executeCallback) {
        return new OutgoingRecordsProducerBuilder<>(executeCallback);
    }

    public OutgoingRecordsProducerBuilder<KeyT, ValueT> topic(String topic) {
        this.topic = topic;
        return this;
    }

    public <S, T> OutgoingRecordsProducerBuilder<KeyT, ValueT> keyValuesSupplier(
            Supplier<Optional<KeyValue<S, T>>> keyValueSupplier) {
        final Stream<KeyValue<S, T>> keyValueStream = StreamUtils.factoryMethodToStream(keyValueSupplier);
        return this.keyValuesStream(keyValueStream);
    }

    public <S, T> OutgoingRecordsProducerBuilder<KeyT, ValueT> keyValuesStream(Stream<KeyValue<S, T>> keyValueStream) {
        return this.recordsStream(keyValueStream.map(this::keyValueToProducerRecord));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <S, T> OutgoingRecordsProducerBuilder<KeyT, ValueT> recordsStream(
            Stream<ProducerRecord<S, T>> recordSupplier) {
        this.recordSupplier = (Stream) recordSupplier;
        return this;
    }

    private <T, S> ProducerRecord<T, S> keyValueToProducerRecord(KeyValue<T, S> keyValue) {
        return new ProducerRecord<>(this.topic, keyValue.getKey(), keyValue.getValue());
    }

    public void sendRecords() {
        this.executeCallback.accept(producer ->
                OutgoingRecordsProducerBuilder.this.recordSupplier.map(producer::send)
                        .map(wrapCheckedFunction(Future::get))
                        .forEachOrdered(recordMetadata -> {
                        }));
    }
}
