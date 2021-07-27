package pl.jch.tests.kafka.main.admin_client;

import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

public class AdminClientMain {
    public static void main(String[] args) {
        final AdminClient adminClient =
                AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
    }
}
