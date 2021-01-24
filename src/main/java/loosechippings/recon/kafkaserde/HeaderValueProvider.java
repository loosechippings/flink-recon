package loosechippings.recon.kafkaserde;

import loosechippings.recon.SerializableFunction;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.function.Function;

public class HeaderValueProvider<T> implements Serializable {

    private static final String SPEC_VERSION = "1.0";
    private final SerializableFunction<T, String> idFunction;
    private final SerializableFunction<T, String> sourceFunction;
    private final SerializableFunction<T, String> typeFunction;
    private final SerializableFunction<T, Long> timestampFunction;
    private transient SimpleDateFormat timestampFormat;

    public HeaderValueProvider(
            SerializableFunction<T, String> idFunction,
            SerializableFunction<T, String> sourceFunction,
            SerializableFunction<T, String> typeFunction,
            SerializableFunction<T, Long> timestampFunction
    ) {
        this.idFunction = idFunction;
        this.sourceFunction = sourceFunction;
        this.typeFunction = typeFunction;
        this.timestampFunction = timestampFunction;
    }

    public Iterable<Header> getHeaders(T element) {
        initializeProvider();
        Header idHeader = new RecordHeader("ce_id", idFunction.apply(element).getBytes());
        Header typeHeader = new RecordHeader("ce_type", typeFunction.apply(element).getBytes());
        Header sourceHeader = new RecordHeader("ce_source", sourceFunction.apply(element).getBytes());
        Long timestamp = timestampFunction.apply(element);
        Header timeHeader = new RecordHeader("ce_time", timestampFormat.format(new Date(timestamp)).getBytes());
        Header specversionHeader = new RecordHeader("ce_specversion", SPEC_VERSION.getBytes());
        return Arrays.asList(idHeader, typeHeader, sourceHeader, timeHeader, specversionHeader);
    }

    private void initializeProvider() {
        if (timestampFormat == null) {
            timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        }
    }

}
