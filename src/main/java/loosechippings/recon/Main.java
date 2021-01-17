package loosechippings.recon;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Integer> intlist = Arrays.asList(1, 2, 3, 4, 5);
        DataStream<Integer> intStream = env.fromCollection(intlist);
        intStream
                .map(i -> i * 2)
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
