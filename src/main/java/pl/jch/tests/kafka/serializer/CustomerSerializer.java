package pl.jch.tests.kafka.serializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import pl.jch.tests.kafka.model.Customer;

import static java.util.Objects.requireNonNullElse;

public class CustomerSerializer implements Serializer<Customer> {

    @Override
    public byte[] serialize(String topic, Customer data) {
        if (data == null) {
            return null;
        }

        try {
            final byte[] nameBytes = requireNonNullElse(data.getName(), "")
                    .getBytes();

            return ByteBuffer.allocate(8 + nameBytes.length)
                    .putInt(data.getId())
                    .putInt(nameBytes.length)
                    .put(nameBytes)
                    .array();
        } catch (RuntimeException e) {
            throw new SerializationException("Error when serializing customer to byte[] " + e.getMessage(), e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }
}
