package loosechippings.recon.kafkaserde;

import loosechippings.domain.avro.Record;
import net.mguenther.kafka.junit.ExternalKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ObserveKeyValues;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ProducerTest {

    Logger LOG = LoggerFactory.getLogger(ProducerTest.class);

    @ClassRule
    public static DockerComposeContainer environment = new DockerComposeContainer(new File("src/test/resources/docker-compose.yml"))
            .withExposedService("broker", 9092)
            .withExposedService("schema-registry", 8081);
    private String brokerUrl;
    private String schemaUrl;
    private ExternalKafkaCluster cluster;

    @Before
    public void setUp() {
        brokerUrl = environment.getServiceHost("broker", 9092) + ":" + environment.getServicePort("broker", 9092);
        schemaUrl = "http://" + environment.getServiceHost("schema-registry", 8081) + ":" + environment.getServicePort("schema-registry", 8081);
        cluster = ExternalKafkaCluster.at(brokerUrl);
    }

    @Test
    public void testProduceSingleRecord() throws Exception {
        Record record = Record.newBuilder()
                .setId("1")
                .setSource("source")
                .setTimestamp(1L)
                .build();
        writeRecord(record);

        List<KeyValue<String, String>> resultList = cluster.observe(ObserveKeyValues.on("test-topic", 1));
        Headers headers = resultList.get(0).getHeaders();
        Header expected = new RecordHeader("ce_id", "1".getBytes());
        assertTrue(StreamSupport.stream(headers.spliterator(), false).anyMatch(expected::equals));
    }

    @Test
    public void testConsumeSingleRecord() throws Exception {
        Record record = Record.newBuilder()
                .setId("1")
                .setSource("source")
                .setTimestamp(1L)
                .build();
        Record record2 = Record.newBuilder(record).setId("EOF").build();
        writeRecord(record, record2);

        CollectSink sink = new CollectSink();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokerUrl);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // unless parallelism is 1 the source won't stop on EOF
        FlinkKafkaConsumer<Record> recordConsumer = new FlinkKafkaConsumer<Record>(
                "test-topic",
                new TestDeserializer<>(schemaUrl, Record.class),
                properties
        );
        recordConsumer.setStartFromEarliest();
        DataStream<Record> recordStream = env.addSource(recordConsumer);
        recordStream.addSink(sink);
        env.execute();
        assertEquals(1, CollectSink.values.size());
    }

    private void writeRecord(Record... elements) throws Exception {
        if (cluster.exists("test-topic")) {
            cluster.deleteTopic("test-topic");
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
        DataStream<Record> recordStream = env.fromElements(elements);
        recordStream.addSink(sink);
        env.execute();
    }

    private static class CollectSink implements SinkFunction<Record> {

        // must be static
        public static final List<Record> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Record value, Context context) throws Exception {
            values.add(value);
        }
    }
}
