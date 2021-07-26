package pl.jch.tests.kafka.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.function.Supplier;

import static pl.jch.tests.kafka.utils.functions.CheckedExceptionUtils.wrapCheckedSupplier;

public class StdIn {

    private static final Supplier<String> LINE_SUPPLIER =
            createStdInLineSupplier();

    private static Supplier<String> createStdInLineSupplier() {
        final InputStreamReader inputReader = new InputStreamReader(System.in);
        final BufferedReader bufferedReader = new BufferedReader(inputReader);
        return wrapCheckedSupplier(bufferedReader::readLine);
    }

    public static String readLine() {
        return LINE_SUPPLIER.get();
    }

}
