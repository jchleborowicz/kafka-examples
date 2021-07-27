package pl.jch.tests.kafka.main.admin_client;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import pl.jch.tests.kafka.utils.builders.AdminClientBuilder;

public class AdminClientMain {

    public static void main(String[] args) {
        AdminClientBuilder.builder()
                .execute(AdminClientMain::execute);
    }

    private static void execute(AdminClient adminClient) throws ExecutionException, InterruptedException {
        adminClient.listTopics()
                .names()
                .get()
                .stream()
                .sorted()
                .forEach(System.out::println);
    }
}
