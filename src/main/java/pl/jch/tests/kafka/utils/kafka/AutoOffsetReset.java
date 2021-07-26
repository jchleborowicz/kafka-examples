package pl.jch.tests.kafka.utils.kafka;

import pl.jch.tests.kafka.utils.IdentifiableEnum;

public enum AutoOffsetReset implements IdentifiableEnum<String> {
    LATEST("latest"),
    EARLIEST("earliest"),
    NONE("none");

    private final String id;

    AutoOffsetReset(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }
}
