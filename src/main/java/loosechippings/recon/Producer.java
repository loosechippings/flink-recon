package loosechippings.recon;

import loosechippings.domain.avro.Record;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class Producer {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Integer> intlist = Arrays.asList(1, 2, 3, 4, 5);

        DataStream<Integer> intStream = env.fromCollection(intlist);
        intStream
                .map(i -> {
                    return Record.newBuilder()
                            .setId(i.toString())
                            .setSource("source")
                            .setTimestamp(Instant.now().toEpochMilli())
                            .build();
                })
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
