package org.phoebus.recceiver;
import org.junit.jupiter.api.Test;
import org.phoebus.recceiver.spi.AddInfoMessage;
import org.phoebus.recceiver.spi.AddRecordMessage;
import org.phoebus.recceiver.spi.ConnectionClosedEvent;
import org.phoebus.recceiver.spi.DelRecordMessage;
import org.phoebus.recceiver.spi.RecSyncTransaction;
import org.phoebus.recceiver.spi.cf.InMemoryRecSyncProcessor;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryRecSyncProcessorTest {
    @Test
    void commitTracksRecordsAliasesAndDeletesByRecId() {
        InMemoryRecSyncProcessor processor = new InMemoryRecSyncProcessor();
        RecSyncTransaction addTx = new RecSyncTransaction(
                "/127.0.0.1:5070",
                List.of(
                        new AddRecordMessage(10, 0, 3, 6, "ai", "pv:one"),
                        new AddRecordMessage(10, 1, 0, 12, "", "pv:one:alias")
                ),
                List.of(),
                List.of(new AddInfoMessage(0, 7, 4, "IOCNAME", "ioc-a")),
                Map.of("IOCNAME", "ioc-a", "HOSTNAME", "host-a"),
                true,
                true
        );
        processor.commit(addTx);
        assertEquals(1, processor.iocCount());
        assertTrue(processor.channelIndexView().containsKey("pv:one"));
        assertTrue(processor.channelIndexView().containsKey("pv:one:alias"));
        // Each channel is served by exactly one IOC
        assertEquals("127.0.0.1:5070", processor.channelIndexView().get("pv:one"));
        assertEquals("127.0.0.1:5070", processor.channelIndexView().get("pv:one:alias"));

        RecSyncTransaction delTx = new RecSyncTransaction(
                "/127.0.0.1:5070",
                List.of(),
                List.of(new DelRecordMessage(10)),
                List.of(),
                Map.of("IOCNAME", "ioc-a", "HOSTNAME", "host-a"),
                false,
                true
        );
        processor.commit(delTx);
        assertFalse(processor.channelIndexView().containsKey("pv:one"));
        assertFalse(processor.channelIndexView().containsKey("pv:one:alias"));
    }
    @Test
    void connectionClosedRemovesIocChannels() {
        InMemoryRecSyncProcessor processor = new InMemoryRecSyncProcessor();
        RecSyncTransaction addTx = new RecSyncTransaction(
                "/127.0.0.1:5080",
                List.of(new AddRecordMessage(20, 0, 3, 6, "ai", "pv:close")),
                List.of(),
                List.of(),
                Map.of(),
                true,
                true
        );
        processor.commit(addTx);
        assertEquals(1, processor.iocCount());
        assertTrue(processor.channelIndexView().containsKey("pv:close"));
        processor.connectionClosed(new ConnectionClosedEvent("/127.0.0.1:5080", "eof"));
        assertEquals(0, processor.iocCount());
        assertFalse(processor.channelIndexView().containsKey("pv:close"));
    }

    @Test
    void channelTakeoverWhenDifferentIocAddsExistingChannel() {
        InMemoryRecSyncProcessor processor = new InMemoryRecSyncProcessor();

        // IOC1 adds channel "pv:moved"
        RecSyncTransaction ioc1Tx = new RecSyncTransaction(
                "/127.0.0.1:5070",
                List.of(new AddRecordMessage(10, 0, 3, 6, "ai", "pv:moved")),
                List.of(),
                List.of(),
                Map.of("IOCNAME", "ioc-a"),
                true,
                true
        );
        processor.commit(ioc1Tx);
        assertEquals("127.0.0.1:5070", processor.channelIndexView().get("pv:moved"));
        assertEquals(1, processor.iocCount());

        // IOC2 adds the same channel "pv:moved" - should take it over
        RecSyncTransaction ioc2Tx = new RecSyncTransaction(
                "/127.0.0.1:5071",
                List.of(new AddRecordMessage(20, 0, 3, 6, "ai", "pv:moved")),
                List.of(),
                List.of(),
                Map.of("IOCNAME", "ioc-b"),
                true,
                true
        );
        processor.commit(ioc2Tx);
        // Channel should now be served by IOC2 only
        assertEquals("127.0.0.1:5071", processor.channelIndexView().get("pv:moved"));
        assertEquals(2, processor.iocCount());
    }
}
