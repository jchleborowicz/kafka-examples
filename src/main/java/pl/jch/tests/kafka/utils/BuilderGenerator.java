package pl.jch.tests.kafka.utils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * Class generates ConsumerBuilder and ProducerBuilder.
 */
public class BuilderGenerator {

    private static final Set<String> IGNORED_CONSTANTS = Set.of(
            "SaslConfigs.DEFAULT_SASL_MECHANISM",
            "CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG",
            "CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG",
            "CommonClientConfigs.METADATA_MAX_AGE_CONFIG",
            "CommonClientConfigs.SEND_BUFFER_CONFIG",
            "CommonClientConfigs.RECEIVE_BUFFER_CONFIG",
            "CommonClientConfigs.CLIENT_ID_CONFIG",
            "CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG",
            "CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG",
            "CommonClientConfigs.RETRIES_CONFIG",
            "CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG",
            "CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG",
            "CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG",
            "CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG",
            "CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG",
            "CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG",
            "CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG",
            "CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG",
            "CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG",
            "CommonClientConfigs.CLIENT_RACK_CONFIG",
            "CommonClientConfigs.GROUP_ID_CONFIG",
            "CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG",
            "CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG",
            "CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG",
            "CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG",
            "CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG");

    private static final Set<String> IGNORED_CONSTANT_VALUES = Set.of("JKS");

    private static final Map<String, String> TYPE_BY_CONFIG_NAME = Map.of(
            CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG, "Sensor.RecordingLevel",
            CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG, "ClientDnsLookup"
    );

    /**
     * These configs have only non-public constants in Kafka library.
     */
    private static final Set<String> CONFIGS_WITHOUT_CONSTANT_NAME = Set.of(
            "internal.auto.downgrade.txn.commit",
            "internal.leave.group.on.close",
            "internal.throw.on.fetch.stable.offset.unsupported"
    );

    private final Map<String, String> constantNameByProperty;
    private final String builderClassName;
    public static final Map<ConfigDef.Type, String> TYPE_BY_CONFIG_TYPE = Map.of(
            ConfigDef.Type.BOOLEAN, "boolean",
            ConfigDef.Type.CLASS, "Class<?>",
            ConfigDef.Type.DOUBLE, "double",
            ConfigDef.Type.INT, "int",
            ConfigDef.Type.LIST, "String",
            ConfigDef.Type.LONG, "long",
            ConfigDef.Type.PASSWORD, "String",
            ConfigDef.Type.SHORT, "short",
            ConfigDef.Type.STRING, "String"
    );

    public BuilderGenerator(String builderClassName, Class<?>... configClasses) {
        this.builderClassName = builderClassName;
        this.constantNameByProperty =
                staticFieldsStream(configClasses)
                        .filter(f -> Modifier.isPublic(f.getModifiers()))
                        .map(field -> ConstantDef.of(getValue(field),
                                field.getDeclaringClass().getSimpleName() + "." + field.getName()))
                        .filter(s -> !s.getConfigName().contains(" "))
                        .filter(s -> !IGNORED_CONSTANTS.contains(s.getConstantName()))
                        .filter(s -> !IGNORED_CONSTANT_VALUES.contains(s.getConfigName()))
                        .collect(toMap(ConstantDef::getConfigName, ConstantDef::getConstantName));
    }

    public static void main(String[] args) throws IOException {
        new BuilderGenerator("ProducerBuilder", ProducerConfig.class, SaslConfigs.class, CommonClientConfigs.class,
                SslConfigs.class)
                .generateBuilder(ProducerConfig.configDef(),
                        "src/main/java/pl/jch/tests/kafka/utils/ProducerBuilder.java");
        new BuilderGenerator("ConsumerBuilder", ConsumerConfig.class, SaslConfigs.class, SslConfigs.class,
                CommonClientConfigs.class)
                .generateBuilder(ConsumerConfig.configDef(),
                        "src/main/java/pl/jch/tests/kafka/utils/ConsumerBuilder.java");
    }

    private void generateBuilder(ConfigDef configDef, String builderFileName) throws IOException {
        final String builderMethods = configDef.configKeys()
                .values()
                .stream()
                .sorted(comparing(configKey -> configKey.name))
                .map(this::createBuilderMethod)
                .collect(joining(""));

        final String collect = "    // ### AUTOGENERATED BUILDER METHODS START ###\n" +
                "    // DO NOT EDIT MANUALLY\n\n"
                + builderMethods +
                "    // ### AUTOGENERATED BUILDER METHODS END ###\n";

        final Path path = Path.of(builderFileName);
        final String content = Files.readString(path);
        final String newContent = content.replaceAll(
                " {4}// ### AUTOGENERATED BUILDER METHODS START ###(.*\\n)* {4}// ### AUTOGENERATED BUILDER METHODS END ###\\n",
                collect);

        Files.writeString(path, newContent);

        System.out.printf("%s done.%n", this.builderClassName);
    }

    private static Stream<Field> staticFieldsStream(Class<?>... classes) {
        return Arrays.stream(classes)
                .flatMap(c -> Arrays.stream(c.getDeclaredFields()))
                .filter(field -> Modifier.isStatic(field.getModifiers()))
                .filter(field -> Modifier.isFinal(field.getModifiers()))
                .filter(field -> field.getType() == String.class);
    }

    private String createBuilderMethod(ConfigDef.ConfigKey configKey) {
        final String configName = configKey.name;
        final String methodName = toMethodName(configName);
        final String variableName = decapitalize(methodName);

        final String constantName = determineConstantName(configName);

        final String type = getConfigValueType(configKey);

        String javaDoc = "";
        if (StringUtils.isNotEmpty(configKey.documentation)) {
            javaDoc = "    /**\n     * "
                    + String.join("\n     * ",
                    splitLines(configKey.documentation, 100))
                    + "\n     */\n";
        }

        return javaDoc
                + String.format("    public %s %s(%s %s) {%n", this.builderClassName, methodName, type, variableName)
                + String.format("        return config(%s, %s);%n", constantName, variableName)
                + "    }\n\n";
    }

    private String getConfigValueType(ConfigDef.ConfigKey configKey) {
        if (Set.of(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)
                .contains(configKey.name)) {
            return "Class<? extends Serializer<?>>";
        }
        if (Set.of(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
                .contains(configKey.name)) {
            return "Class<? extends Deserializer<?>>";
        }
        if (TYPE_BY_CONFIG_NAME.containsKey(configKey.name)) {
            return TYPE_BY_CONFIG_NAME.get(configKey.name);
        }
        if (configKey.validator != null && configKey.validator.getClass() == ConfigDef.ValidString.class) {
            return toCamelCase(configKey.name);
        }

        final String type = TYPE_BY_CONFIG_TYPE.get(configKey.type());
        if (type == null) {
            throw new RuntimeException("Unexpected type " + configKey.type() + " for property " + configKey.name);
        }
        return type;
    }

    private static String toCamelCase(String text) {
        return Arrays.stream(text.split("\\."))
                .map(String::trim)
                .filter(s -> s.length() > 0)
                .map(BuilderGenerator::capitalize)
                .collect(joining(""));
    }

    private static List<String> splitLines(String text, int maxLength) {
        text = text.trim();

        final List<String> result = new ArrayList<>();

        while (true) {
            if (text.length() <= maxLength) {
                result.add(text);
                break;
            }
            int index = text.substring(0, maxLength + 1).lastIndexOf(' ');
            if (index < 0) {
                index = text.indexOf(' ');
            }
            if (index < 0) {
                result.add(text);
                break;
            }
            result.add(text.substring(0, index).trim());
            text = text.substring(index).trim();
        }

        return result;
    }

    private String determineConstantName(String configName) {
        if (CONFIGS_WITHOUT_CONSTANT_NAME.contains(configName)) {
            return "\"" + configName + "\"";
        }
        final String result = this.constantNameByProperty.get(configName);
        if (result == null) {
            throw new RuntimeException("Cannot find constant for property " + configName);
        }
        return result;
    }

    static String toMethodName(String propertyName) {
        final String result = Arrays.stream(propertyName.split("\\."))
                .map(BuilderGenerator::capitalize)
                .collect(joining(""));
        return decapitalize(result);
    }

    private static String decapitalize(String result) {
        return result.substring(0, 1).toLowerCase() + result.substring(1);
    }

    private static String capitalize(String s) {
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }

    private static String getValue(Field field) {
        try {
            return (String) field.get(null);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Value
    @AllArgsConstructor(staticName = "of")
    static class ConstantDef {
        String configName;
        String constantName;
    }

}
