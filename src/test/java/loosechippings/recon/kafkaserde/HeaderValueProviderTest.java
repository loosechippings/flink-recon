package loosechippings.recon.kafkaserde;

import loosechippings.domain.avro.Record;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class HeaderValueProviderTest {

    @Test
    public void testCreateHeaders() {
        HeaderValueProvider<Record> headerValueProvider = new HeaderValueProvider<>(
                Record::getId,
                Record::getSource,
                t -> t.getSchema().getFullName(),
                Record::getTimestamp
        );
        HeaderAwareKafkaAvroSerializationSchema<Record> subject = new HeaderAwareKafkaAvroSerializationSchema<>(
                "topic",
                Collections.singletonList("url"),
                Record.class,
                Record::getId,
                headerValueProvider
        );
        Record record = Record.newBuilder()
                .setTimestamp(1L)
                .setId("id1")
                .setSource("source")
                .build();
        Iterable<Header> headers = headerValueProvider.getHeaders(record);
        Header idHeader = new RecordHeader("ce_id", "id1".getBytes());
        Header typeHeader = new RecordHeader("ce_type", "loosechippings.domain.avro.Record".getBytes());
        Header sourceHeader = new RecordHeader("ce_source", "source".getBytes());
        Header timestampHeader = new RecordHeader("ce_time", "1970-01-01T01:00:00+01:00".getBytes());
        Header specversionHeader = new RecordHeader("ce_specversion", "1.0".getBytes());
        Iterable<Header> expectedHeaders = Arrays.asList(idHeader, typeHeader, sourceHeader, timestampHeader, specversionHeader);
        assertEquals(expectedHeaders, headers);
    }

}