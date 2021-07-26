package pl.jch.tests.kafka.utils.kafka;

import pl.jch.tests.kafka.utils.IdentifiableEnum;

public enum Acks implements IdentifiableEnum<String> {
    NO_WAIT("0"),
    ONLY_LEADER("1"),
    ALL("all");

    private final String id;

    Acks(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return this.id;
    }
}
