package pl.jch.tests.kafka.utils;

import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;

import static pl.jch.tests.kafka.utils.functions.CheckedExceptionUtils.wrapCheckedFunction;

@UtilityClass
public class LoggingUtils {

    public static final Function<Object, String> JSON_SERIALIZER =
            wrapCheckedFunction(
                    new ObjectMapper().writerWithDefaultPrettyPrinter()::writeValueAsString
            );

    public static void prettyPrint(Map<String, Integer> custCountryMap) {
        final String serialized = JSON_SERIALIZER.apply(custCountryMap);
        System.out.println(serialized);
    }
}
