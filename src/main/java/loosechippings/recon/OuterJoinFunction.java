package loosechippings.recon;

import loosechippings.recon.domain.Audit;
import loosechippings.recon.domain.Event;
import loosechippings.recon.domain.RecRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class OuterJoinFunction extends KeyedCoProcessFunction<String, Event, Audit, RecRecord> {
    List<Event> events;
    List<Audit> audits;

    @Override
    public void open(Configuration config) {
        // todo - these need to use Flink state
        // will write a test before implementing
        events = new ArrayList<>();
        audits = new ArrayList<>();
    }

    @Override
    public void processElement1(Event value, Context ctx, Collector<RecRecord> out) throws Exception {
        events.add(value);
        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
    }

    @Override
    public void processElement2(Audit value, Context ctx, Collector<RecRecord> out) throws Exception {
        audits.add(value);
        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RecRecord> out) throws Exception {
        List<RecRecord> rec = new ArrayList<>();
        List<Event> remainingEvents = new ArrayList<>();
        List<Audit> remainingAudits = new ArrayList<>();
        for (Audit a: audits) {
            Event e = new Event(a.getId(), a.getTimestamp());
            if (a.getTimestamp() <= timestamp && events.contains(e)) {
                rec.add(new RecRecord(e, a));
            } else {
                remainingAudits.add(a);
                remainingEvents.add(e);
            }
        }
        events = remainingEvents;
        audits = remainingAudits;
        rec.forEach(out::collect);
    }

}
