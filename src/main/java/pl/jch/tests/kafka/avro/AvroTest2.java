package pl.jch.tests.kafka.avro;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import example.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroTest2 {

    public static final File FILE = new File("users.avro");

    public static void main(String[] args) throws IOException {
//        write();
        read();
    }

    private static <T> void onEach(DataFileReader<T> dataFileReader, Consumer<T> callback) throws IOException {
        T object = null;
        while (dataFileReader.hasNext()) {
            object = dataFileReader.next(object);
            callback.accept(object);
        }
    }

    private static void read() throws IOException {
        final DatumReader<User> userDatumReader = new SpecificDatumReader<>(User.class);
        try (final DataFileReader<User> dataFileReader = new DataFileReader<>(FILE, userDatumReader)) {
            onEach(dataFileReader, System.out::println);
        }
    }

    private static void write() throws IOException {
        final User user1 = new User();
        user1.setName("Alyssa");
        user1.setFavoriteNumber(256);

        final User user2 = new User("Ben", 7, "red");

        final User user3 = User.newBuilder()
                .setName("Charlie")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();

        final DatumWriter<User> userSpecificDatumWriter = new SpecificDatumWriter<>(User.class);
        try (final DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userSpecificDatumWriter)) {
            dataFileWriter.create(user1.getSchema(), FILE);
            dataFileWriter.append(user1);
            dataFileWriter.append(user2);
            dataFileWriter.append(user3);
        }
    }
}
