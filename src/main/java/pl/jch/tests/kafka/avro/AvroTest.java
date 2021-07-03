package pl.jch.tests.kafka.avro;

import java.util.List;

import lombok.Data;

public class AvroTest {


}

@Data
class AvroHttpRequest {
    private long requestTime;
    private ClientIdentifier clientIdentifier;
    private List<String> employeeNames;
    private Active active;
}
