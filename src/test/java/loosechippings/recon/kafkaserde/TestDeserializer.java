package loosechippings.recon.kafkaserde;

import loosechippings.domain.avro.Record;

public class TestDeserializer<T> extends HeaderAwareKafkaAvroDeserializationSchema<T> {

    public TestDeserializer(String schemaUrl, Class clazz) {
        super(schemaUrl, clazz);
    }

    @Override
    public boolean isEndOfStream(T record) {
        Record r = (Record)record;
        if (r.getId().equals("EOF")) {
            return true;
        }
        return false;
    }
}
