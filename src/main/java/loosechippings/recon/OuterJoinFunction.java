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
    List<Event> unmatchedEvents;
    List<Audit> unmatchedAudits;

    @Override
    public void open(Configuration config) {
        // todo - these need to use Flink state
        // will write a test before implementing
        events = new ArrayList<>();
        audits = new ArrayList<>();
        unmatchedEvents = new ArrayList<>();
        unmatchedAudits = new ArrayList<>();
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
        List<Audit> remainingUnmatchedAudits = new ArrayList<>();

        matchAudits(timestamp, rec, remainingEvents, remainingAudits);
        matchUnmatchedAudits(rec, remainingUnmatchedAudits);

        events = remainingEvents;
        audits = remainingAudits;
        unmatchedAudits = remainingUnmatchedAudits;
        rec.forEach(out::collect);
    }

    /*
     * loop through unmatched Audits looking for late Events
     * don't emit an exception is unmatched - one has already been raised
     */
    private void matchUnmatchedAudits(List<RecRecord> rec, List<Audit> remainingUnmatchedAudits) {
        for (Audit a: unmatchedAudits) {
            Event e = new Event(a.getId(), a.getTimestamp());
            if (events.contains(e)) {
                rec.add(new RecRecord(e, a));
            } else {
                remainingUnmatchedAudits.add(a);
            }
        }
    }

    /*
     * loop through pending Audits looking for matching Events
     * emit a RecRecord - Event will be null if no matching Event found
     */
    private void matchAudits(long timestamp, List<RecRecord> rec, List<Event> remainingEvents, List<Audit> remainingAudits) {
        for (Audit a: audits) {
            Event e = new Event(a.getId(), a.getTimestamp());
            if (a.getTimestamp() <= timestamp) {
                if (events.contains(e)) {
                    rec.add(new RecRecord(e, a));
                } else {
                    // emit one sided rec and keep the unmatched audit
                    rec.add(new RecRecord(null, a));
                    unmatchedAudits.add(a);
                }
            } else {
                remainingAudits.add(a);
                remainingEvents.add(e);
            }
        }
    }

}
