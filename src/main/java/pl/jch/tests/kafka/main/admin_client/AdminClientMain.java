package pl.jch.tests.kafka.main.admin_client;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import pl.jch.tests.kafka.utils.builders.AdminClientBuilder;

import static java.util.Collections.singletonList;

public class AdminClientMain {

    public static final String TOPIC = "test";
    public static final short REPLICATION_FACTOR = (short) 1;
    public static final int NUM_PARTITIONS = 1;

    public static void main(String[] args) {
        AdminClientBuilder.builder()
                .buildAndExecute(AdminClientMain::execute);
    }

    private static void execute(AdminClient adminClient) throws ExecutionException, InterruptedException {
        final DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(singletonList(TOPIC));
        try {
            final TopicDescription topicDescription = describeTopicsResult.values()
                    .get(TOPIC)
                    .get();

            System.out.printf("Partitions count: %s%n", topicDescription.partitions().size());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {// if we are here, topic doesn't exist
                System.out.printf("Topic %s does not exist. Going to create it now", TOPIC);
                createPartition(adminClient);
            } else {
                e.printStackTrace();
                throw e;
            }

        }
    }

    private static void createPartition(AdminClient adminClient) throws InterruptedException, ExecutionException {
        final CreateTopicsResult newTopic =
                adminClient.createTopics(singletonList(new NewTopic(TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR)));

        if (newTopic.numPartitions(TOPIC).get() != NUM_PARTITIONS) {
            System.out.println("Topic has wrong number of partitions.");
            System.exit(-1);
        }
    }
}
