package loosechippings.recon.kafkaserde;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentSchemaRegistryCoder;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.WrappingRuntimeException;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public class HeaderAwareKafkaAvroSerializationSchema<T> implements KafkaSerializationSchema<T> {

    private static final int ID_MAP_CAPACITY = 1000;
    private final Class<T> clazz;
    private final HeaderValueProvider<T> headerValueProvider;
    private final Function<T, String> keyFunction;
    private List<String> urls;
    private ConfluentSchemaRegistryCoder coder;
    private transient ByteArrayOutputStream outputStream;
    private transient BinaryEncoder encoder;
    private Schema schema;
    private SpecificDatumWriter<T> datumWriter;
    private String topic;

    public HeaderAwareKafkaAvroSerializationSchema(String topic, List<String> urls, Class<T> clazz, Function<T, String> keyFunction, HeaderValueProvider headerValueProvider) {
        this.urls = urls;
        this.clazz = clazz;
        this.topic = topic;
        this.headerValueProvider = headerValueProvider;
        this.keyFunction = keyFunction;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        if (coder == null) {
            ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
            EncoderFactory.get().directBinaryEncoder(arrayOutputStream, null);
            SchemaRegistryClient client = new CachedSchemaRegistryClient(urls, ID_MAP_CAPACITY);
            coder = new ConfluentSchemaRegistryCoder(topic, client);
            schema = SpecificData.get().getSchema(clazz);
            datumWriter = new SpecificDatumWriter<T>(clazz);
        }
    }


    @Override
    public ProducerRecord<byte[], byte[]> serialize(T element, Long timestamp) {
        if (element == null) {
            return null;
        } else {
            try {
                outputStream.reset();
                coder.writeSchema(null, outputStream);
                datumWriter.write(element, encoder);
                encoder.flush();
                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, keyFunction.apply(element).getBytes(), outputStream.toByteArray());
                headerValueProvider.getHeaders(element).forEach(producerRecord.headers()::add);
                return producerRecord;
            } catch (IOException e) {
                throw new WrappingRuntimeException("Failed to serialize schema registry.", e);
            }
        }
    }
}
