package loosechippings.recon.kafkaserde;

import loosechippings.domain.avro.Record;
import net.mguenther.kafka.junit.ExternalKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ObserveKeyValues;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertTrue;

public class ProducerTest {

    @ClassRule
    public static DockerComposeContainer environment = new DockerComposeContainer(new File("src/test/resources/docker-compose.yml"))
            .withExposedService("broker", 9092)
            .withExposedService("schema-registry", 8081);
    private String brokerUrl;
    private String schemaUrl;

    @Before
    public void setUp() {
        brokerUrl = environment.getServiceHost("broker", 9092) + ":" + environment.getServicePort("broker", 9092);
        schemaUrl = "http://" + environment.getServiceHost("schema-registry", 8081) + ":" + environment.getServicePort("schema-registry", 8081);
    }

    @Test
    public void testProduceSingleRecord() throws Exception {
        ExternalKafkaCluster cluster = ExternalKafkaCluster.at(brokerUrl);
        if (cluster.exists("test-topic")) {
            cluster.deleteTopic("test-topic");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        HeaderValueProvider<Record> headerValueProvider = new HeaderValueProvider<>(
                Record::getId,
                Record::getSource,
                r -> r.getSchema().getFullName(),
                Record::getTimestamp
            );
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokerUrl);
        FlinkKafkaProducer<Record> sink = new FlinkKafkaProducer<Record>(
                "localhost:9091",
                new HeaderAwareKafkaAvroSerializationSchema<Record>(
                        "test-topic",
                        Collections.singletonList(schemaUrl),
                        Record.class,
                        Record::getId,
                        headerValueProvider
                ),
                properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
        Record record = Record.newBuilder()
                .setId("1")
                .setSource("source")
                .setTimestamp(1L)
                .build();
        DataStream<Record> recordStream = env.fromElements(record);
        recordStream.addSink(sink);
        env.execute();

        List<KeyValue<String, String>> resultList = cluster.observe(ObserveKeyValues.on("test-topic", 1));
        Headers headers = resultList.get(0).getHeaders();
        Header expected = new RecordHeader("ce_id", "1".getBytes());
        assertTrue(StreamSupport.stream(headers.spliterator(), false).anyMatch(expected::equals));
    }
}
