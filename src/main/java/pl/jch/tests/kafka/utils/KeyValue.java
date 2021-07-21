package pl.jch.tests.kafka.utils;

import lombok.Value;

@Value(staticConstructor = "of")
public class KeyValue<KeyT, ValueT> {
    KeyT key;
    ValueT value;
}
