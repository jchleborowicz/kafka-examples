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

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static pl.jch.tests.kafka.utils.functions.CheckedExceptionUtils.wrapCheckedSupplier;

/**
 * Class generates ConsumerBuilder and ProducerBuilder.
 */
public class BuilderGeneratorMain {

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
            "CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG",
            "AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY_DEFAULT",
            "AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY_DEFAULT");

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
            "internal.throw.on.fetch.stable.offset.unsupported",
            "schema.registry.ssl.cipher.suites",
            "schema.registry.ssl.enabled.protocols",
            "schema.registry.ssl.endpoint.identification.algorithm",
            "schema.registry.ssl.engine.factory.class",
            "schema.registry.ssl.key.password",
            "schema.registry.ssl.keymanager.algorithm",
            "schema.registry.ssl.keystore.certificate.chain",
            "schema.registry.ssl.keystore.key",
            "schema.registry.ssl.keystore.location",
            "schema.registry.ssl.keystore.password",
            "schema.registry.ssl.keystore.type",
            "schema.registry.ssl.protocol",
            "schema.registry.ssl.provider",
            "schema.registry.ssl.secure.random.implementation",
            "schema.registry.ssl.trustmanager.algorithm",
            "schema.registry.ssl.truststore.certificates",
            "schema.registry.ssl.truststore.location",
            "schema.registry.ssl.truststore.password",
            "schema.registry.ssl.truststore.type"
    );

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

    private final BuilderGeneratorInput builderGeneratorInput;
    private final Map<String, String> constantNameByProperty;

    public BuilderGeneratorMain(BuilderGeneratorInput builderGeneratorInput) {
        this.builderGeneratorInput = builderGeneratorInput;
        this.constantNameByProperty =
                staticFieldsStream(builderGeneratorInput.getConstantsClasses())
                        .filter(field -> Modifier.isPublic(field.getModifiers()))
                        .map(field -> ConstantDef.of(getValue(field),
                                field.getDeclaringClass().getSimpleName() + "." + field.getName()))
                        .filter(constantDef -> StringUtils.isNotBlank(constantDef.getConfigName()))
                        .filter(constantDef -> !constantDef.getConfigName().contains(" "))
                        .filter(constantDef -> !IGNORED_CONSTANTS.contains(constantDef.getConstantName()))
                        .filter(constantDef -> !IGNORED_CONSTANT_VALUES.contains(constantDef.getConfigName()))
                        .collect(toMap(ConstantDef::getConfigName, ConstantDef::getConstantName));
    }

    public static void main(String[] args) throws IOException, NoSuchFieldException, IllegalAccessException {
        BuilderGeneratorInput.builder()
                .configDefs(asList(ProducerConfig.configDef(), getAvroSerializerConfigDef()))
                .builderClassName("ProducerBuilder")
                .builderFileName("src/main/java/pl/jch/tests/kafka/utils/builders/ProducerBuilder.java")
                .constantsClasses(
                        asList(ProducerConfig.class, SaslConfigs.class, CommonClientConfigs.class, SslConfigs.class,
                                KafkaAvroSerializerConfig.class, AbstractKafkaSchemaSerDeConfig.class))
                .build()
                .generate();

        BuilderGeneratorInput.builder()
                .configDefs(asList(ConsumerConfig.configDef(), getAvroDeserializerConfigDef()))
                .builderClassName("ConsumerBuilder")
                .builderFileName("src/main/java/pl/jch/tests/kafka/utils/builders/ConsumerBuilder.java")
                .constantsClasses(
                        asList(ConsumerConfig.class, SaslConfigs.class, SslConfigs.class, CommonClientConfigs.class,
                                KafkaAvroDeserializerConfig.class, AbstractKafkaSchemaSerDeConfig.class))
                .build()
                .generate();

        BuilderGeneratorInput.builder()
                .configDefs(singletonList(AdminClientConfig.configDef()))
                .builderClassName("AdminClientBuilder")
                .builderFileName("src/main/java/pl/jch/tests/kafka/utils/builders/AdminClientBuilder.java")
                .constantsClasses(asList(AdminClientConfig.class, SaslConfigs.class, SslConfigs.class))
                .build()
                .generate();
    }

    private static ConfigDef getAvroSerializerConfigDef() throws NoSuchFieldException, IllegalAccessException {
        final Field config = KafkaAvroSerializerConfig.class.getDeclaredField("config");
        config.setAccessible(true);
        return (ConfigDef) config.get(null);
    }

    private static ConfigDef getAvroDeserializerConfigDef() throws NoSuchFieldException, IllegalAccessException {
        final Field config = KafkaAvroDeserializerConfig.class.getDeclaredField("config");
        config.setAccessible(true);
        return (ConfigDef) config.get(null);
    }

    @Value
    @AllArgsConstructor(staticName = "of")
    static class ConfigNameDef {
        String namespace;
        ConfigDef.ConfigKey configKey;
    }

    private void generate() throws IOException {
        final String builderMethods = this.builderGeneratorInput.getConfigDefs()
                .stream()
                .flatMap(configDef -> configDef
                        .configKeys()
                        .values()
                        .stream()
                )
                .sorted(comparing(configKey -> configKey.name))
                .map(this::createBuilderMethod)
                .collect(joining(""));

        final String collect = "    // ### AUTOGENERATED BUILDER METHODS START ###\n" +
                "    // DO NOT EDIT MANUALLY\n\n"
                + builderMethods +
                "    // ### AUTOGENERATED BUILDER METHODS END ###\n";

        final Path path = Path.of(this.builderGeneratorInput.getBuilderFileName());
        final String content = Files.readString(path);
        final String newContent = content.replaceAll(
                " {4}// ### AUTOGENERATED BUILDER METHODS START ###(.*\\n)* {4}// ### AUTOGENERATED BUILDER METHODS END ###\\n",
                collect);

        Files.writeString(path, newContent);

        System.out.printf("%s done.%n", this.builderGeneratorInput.getBuilderClassName());
    }

    private static Stream<Field> staticFieldsStream(List<Class<?>> classes) {
        return classes.stream()
                .flatMap(constantsClass -> Arrays.stream(constantsClass.getDeclaredFields()))
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
            final List<String> lines = splitLines(configKey.documentation, 100);
            javaDoc = String.format("    /**\n     * %s\n     */\n",
                    String.join("\n     * ", lines));
        }

        final String builderClassName = this.builderGeneratorInput.getBuilderClassName();
        return javaDoc
                + String
                .format("    public %s %s(%s %s) {%n", builderClassName, methodName, type, variableName)
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
                .map(BuilderGeneratorMain::capitalize)
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
                .map(BuilderGeneratorMain::capitalize)
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
        return wrapCheckedSupplier(() -> (String) field.get(null))
                .get();
    }

    @Value
    @Builder
    static class BuilderGeneratorInput {
        String builderClassName;
        List<Class<?>> constantsClasses;
        List<ConfigDef> configDefs;
        String builderFileName;

        void generate() throws IOException {
            new BuilderGeneratorMain(this)
                    .generate();
        }
    }

    @Value
    @AllArgsConstructor(staticName = "of")
    static class ConstantDef {
        String configName;
        String constantName;
    }

}
