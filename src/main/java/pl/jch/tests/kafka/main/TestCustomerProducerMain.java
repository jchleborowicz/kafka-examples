package pl.jch.tests.kafka.main;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import pl.jch.tests.kafka.model.Customer;
import pl.jch.tests.kafka.serializer.CustomerSerializer;
import pl.jch.tests.kafka.utils.ProducerBuilder;
import pl.jch.tests.kafka.utils.Topics;

public class TestCustomerProducerMain {

    public static final String TOPIC = Topics.TEST;

    public static void main(String[] args) {
        ProducerBuilder.builder(StringSerializer.class, CustomerSerializer.class)
                .execute(TestCustomerProducerMain::execute);
    }

    private static void execute(Producer<String, Customer> producer) throws ExecutionException, InterruptedException {
        final ProducerRecord<String, Customer> record = new ProducerRecord<>(TOPIC, new Customer(2, "Jacek"));
        System.out.println(
                producer.send(record).get()
        );
    }
}
