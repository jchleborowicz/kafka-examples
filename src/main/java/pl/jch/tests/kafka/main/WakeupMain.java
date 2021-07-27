package pl.jch.tests.kafka.main;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import pl.jch.tests.kafka.utils.builders.ConsumerBuilder;

public class WakeupMain {

    public static final String TOPIC = "test";

    public static void main(String[] args) {
        ConsumerBuilder.builder()
                .groupId("counter")
                .enableAutoCommit(false)
                .defineIncomingRecordsHandler()
                .pollDuration(Duration.ofMillis(100))
                .topic(TOPIC)
                .onRecord(getConsumeRecord())
                .pollInfinitely();
    }

    private static Consumer<ConsumerRecord<Void, String>> getConsumeRecord() {
        final ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(3);

        return record -> {
            try {
                final int number = Integer.parseInt(record.value());
                while (queue.remainingCapacity() <= 0) {
                    queue.poll();
                }
                queue.add(number);
            } catch (NumberFormatException ignored) {
            }

            final int sum = queue.stream()
                    .mapToInt(Integer::intValue)
                    .sum();

            System.out.printf("Moving avg is: %d%n", sum / queue.size());
        };
    }

}
