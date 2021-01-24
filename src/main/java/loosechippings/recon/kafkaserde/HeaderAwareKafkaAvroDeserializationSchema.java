package loosechippings.recon.kafkaserde;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.SchemaCoder;
import org.apache.flink.formats.avro.registry.confluent.ConfluentSchemaRegistryCoder;
import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class HeaderAwareKafkaAvroDeserializationSchema<T> implements KafkaDeserializationSchema<T> {

    private static final int ID_MAP_CAPACITY = 1000;
    private final String schemaUrl;
    private final Class clazz;
    private MutableByteArrayInputStream inputStream;
    private SchemaCoder schemaCoder;
    private GenericDatumReader<T> datumReader;
    private Decoder decoder;

    public HeaderAwareKafkaAvroDeserializationSchema(String schemaUrl, Class clazz) {
        this.schemaUrl = schemaUrl;
        this.clazz = clazz;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {

    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    void initialize() {
        if (inputStream == null) {

            inputStream = new MutableByteArrayInputStream();
            schemaCoder = new ConfluentSchemaRegistryCoder(new CachedSchemaRegistryClient(schemaUrl, ID_MAP_CAPACITY));
            datumReader = new SpecificDatumReader<T>();
            decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        }
    }

    @Override
    public T deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        initialize();
        inputStream.setBuffer(record.value());
        Schema writerSchema = schemaCoder.readSchema(inputStream);
        Schema readerSchema = SpecificData.get().getSchema(clazz);

        datumReader.setSchema(writerSchema);
        datumReader.setExpected(readerSchema);

        return datumReader.read(null, decoder);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(clazz);
    }
}
