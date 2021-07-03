package pl.jch.tests.kafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

@AllArgsConstructor
public class ProducerMain {

    private final Producer<String, String> producer;

    public static void main(String[] args) throws Exception {
        try (final Producer<String, String> producer = KafkaUtils.createProducer()) {
            final ProducerMain test = new ProducerMain(producer);
            test.test();
        }
    }

    private void test() throws Exception {
        final BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            final String line = in.readLine();
            if (line == null || line.equals("x")) {
                break;
            }

            producer.send(new ProducerRecord<>("test", line));
        }
    }

}
