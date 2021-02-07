package loosechippings.recon.control;

import loosechippings.domain.avro.ControlItem;
import loosechippings.domain.avro.ControlItemType;
import loosechippings.domain.avro.EventStoreType;
import loosechippings.domain.avro.InventoryItem;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class InventoryItemToControlItemTransformTest {

    private InventoryItemToControlItemTransformer transformer;

    @Before
    public void setup() {
        this.transformer = new InventoryItemToControlItemTransformer();
    }

    @Test
    public void producesActualControlItem() {
        InventoryItem item = InventoryItem.newBuilder()
                .setEventStore("topic1")
                .setEventStoreType(EventStoreType.TOPIC)
                .setEventId("1")
                .setEventSource("source1")
                .setEventTimestamp(1L)
                .setWriteTimestamp(1L)
                .build();
        List<ControlItem> result = transformer.transform(item);

        ControlItem actual = ControlItem.newBuilder()
                .setControlItemType(ControlItemType.ACTUAL)
                .setEventId("1")
                .setEventSource("source1")
                .setEventTimestamp(1L)
                .build();
        List<ControlItem> expected = Collections.singletonList(actual);

        assertEquals(expected, result);
    }

    @Test
    public void producesExpectedControlItem() {

    }
}
