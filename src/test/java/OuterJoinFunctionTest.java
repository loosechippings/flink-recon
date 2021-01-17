import loosechippings.recon.OuterJoinFunction;
import loosechippings.recon.domain.Audit;
import loosechippings.recon.domain.Event;
import loosechippings.recon.domain.RecRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class OuterJoinFunctionTest {

    private OuterJoinFunction outerJoinFunction;
    private KeyedTwoInputStreamOperatorTestHarness<String, Event, Audit, RecRecord> testHarness;

    @Before
    public void setupTestHarness() throws Exception {
        outerJoinFunction = new OuterJoinFunction();
        testHarness = new KeyedTwoInputStreamOperatorTestHarness<String, Event, Audit, RecRecord>(
                new KeyedCoProcessOperator<String, Event, Audit, RecRecord>(outerJoinFunction),
                Event::getId,
                Audit::getId,
                TypeInformation.of(String.class)
        );
        testHarness.open();
    }

    @Test
    public void testJoinsAnEventAndAudit() throws Exception {
        Event e1 = new Event("1", 1L);
        Audit a1 = new Audit("1", 1L);
        testHarness.processElement1(e1, 1L);
        testHarness.processElement2(a1, 1L);
        testHarness.processBothWatermarks(new Watermark(1L));
        List<RecRecord> results = testHarness.extractOutputValues();
        List<RecRecord> expected = Collections.singletonList(new RecRecord(e1, a1));
        assertThat(results, is(expected));
    }

    @Test
    public void testJoinsSomeEventsAndAudits() throws Exception {
        Event e1 = new Event("1", 1L);
        Event e2 = new Event("2", 2L);
        Audit a1 = new Audit("1", 1L);
        Audit a2 = new Audit("2", 2L);
        testHarness.processElement1(e1, 1L);
        testHarness.processElement1(e2, 2L);
        testHarness.processElement2(a1, 1L);
        testHarness.processElement2(a2, 2L);
        testHarness.processBothWatermarks(new Watermark(2L));
        List<RecRecord> results = testHarness.extractOutputValues();
        List<RecRecord> expected = new ArrayList<>();
        expected.add(new RecRecord(e1, a1));
        expected.add(new RecRecord(e2, a2));
        assertThat(results, is(expected));
    }

    @Test
    public void testJoinsEventsOutOfOrderWithinSameWatermark() throws Exception {
        Event e1 = new Event("1", 1L);
        Event e2 = new Event("2", 2L);
        Audit a1 = new Audit("1", 1L);
        Audit a2 = new Audit("2", 2L);
        testHarness.processElement1(e1, 1L);
        testHarness.processElement1(e2, 2L);
        // put a2 in the stream before a1
        testHarness.processElement2(a2, 1L);
        testHarness.processElement2(a1, 2L);
        testHarness.processBothWatermarks(new Watermark(2L));
        List<RecRecord> results = testHarness.extractOutputValues();
        List<RecRecord> expected = new ArrayList<>();
        expected.add(new RecRecord(e1, a1));
        expected.add(new RecRecord(e2, a2));
        assertThat(results, is(expected));
    }

    @Test
    public void testJoinsWithMultipleWatermarks() throws Exception {
        Event e1 = new Event("1", 1L);
        Event e2 = new Event("2", 2L);
        Audit a1 = new Audit("1", 1L);
        Audit a2 = new Audit("2", 2L);
        testHarness.processElement1(e1, 1L);
        testHarness.processElement1(e2, 2L);
        testHarness.processElement2(a1, 1L);
        testHarness.processElement2(a2, 2L);

        testHarness.processBothWatermarks(new Watermark(1L));
        List<RecRecord> results = testHarness.extractOutputValues();
        assertThat("first watermark", results, is(Collections.singletonList(new RecRecord(e1, a1))));

        testHarness.processBothWatermarks(new Watermark(2L));
        results = testHarness.extractOutputValues();
        List<RecRecord> expected = new ArrayList<>();
        expected.add(new RecRecord(e1, a1));
        expected.add(new RecRecord(e2, a2));
        assertThat("second watermark", results, is(expected));
    }

    @Test
    public void testReturnsOneSidedRecForMissingEvent() throws Exception {
        Audit a1 = new Audit("1", 1L);
        testHarness.processElement2(a1, 1L);
        testHarness.processBothWatermarks(new Watermark(1L));
        List<RecRecord> results = testHarness.extractOutputValues();
        List<RecRecord> expected = Collections.singletonList(new RecRecord(null, a1));
        assertThat(results, is(expected));
    }

    @Test
    public void testEventReportedMissingIsSubsequentlyFound() throws Exception {
        Audit a1 = new Audit("1", 1L);
        testHarness.processElement2(a1, 1L);
        testHarness.processBothWatermarks(new Watermark(1L));
        List<RecRecord> results = testHarness.extractOutputValues();
        List<RecRecord> expected = Collections.singletonList(new RecRecord(null, a1));
        assertThat("event is missing", results, is(expected));

        Event e1 = new Event("1", 1L);
        testHarness.processElement1(e1, 1L);
        testHarness.processBothWatermarks(new Watermark(2L));
        results = testHarness.extractOutputValues();
        expected = new ArrayList<>();
        expected.add(new RecRecord(null, a1));
        expected.add(new RecRecord(e1, a1));
        assertThat("late event matches", results, is(expected));
    }

}
