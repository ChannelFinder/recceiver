package org.phoebus.recceiver;

import org.junit.jupiter.api.Test;
import org.phoebus.recceiver.spi.AddInfoMessage;
import org.phoebus.recceiver.spi.AddRecordMessage;
import org.phoebus.recceiver.spi.DelRecordMessage;
import org.phoebus.recceiver.spi.RecSyncTransaction;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecSyncTransactionMappingTest {

    @Test
    void normalizeCommitBuildsCfstoreLikeViews() {
        RecSyncTransaction tx = new RecSyncTransaction(
                "/127.0.0.1:5070",
                List.of(
                        new AddRecordMessage(100, 0, 3, 4, "ai", "pv:one"),
                        new AddRecordMessage(100, 1, 0, 8, "", "pv:one:alias")
                ),
                List.of(new DelRecordMessage(300)),
                List.of(
                        new AddInfoMessage(0, 7, 4, "IOCNAME", "ioc1"),
                        new AddInfoMessage(100, 4, 3, "DESC", "abc")
                ),
                Map.of("IOCNAME", "ioc1", "HOSTNAME", "host1"),
                true,
                true
        );

        assertTrue(tx.sourceAddressParsed());
        assertEquals("127.0.0.1", tx.sourceHost());
        assertEquals(5070, tx.sourcePort());

        assertEquals(1, tx.recordsToAdd().size());
        assertEquals("pv:one", tx.recordsToAdd().get(100).recordName());
        assertEquals("ai", tx.recordsToAdd().get(100).recordType());

        assertEquals(List.of("pv:one:alias"), tx.aliases().get(100));

        assertEquals("abc", tx.recordInfosToAdd().get(100).get("DESC"));
        assertEquals(1, tx.recordsToDelete().size());
        assertTrue(tx.recordsToDelete().contains(300));

        assertTrue(tx.initial());
        assertTrue(tx.connected());
    }

    @Test
    void normalizedFlagsCanRepresentReconnectSemantics() {
        RecSyncTransaction tx = new RecSyncTransaction(
                "/127.0.0.1:5071",
                List.of(new AddRecordMessage(1, 0, 3, 4, "ai", "pv:a")),
                List.of(),
                List.of(),
                Map.of(),
                false,
                false
        );

        assertFalse(tx.initial());
        assertFalse(tx.connected());
    }

    @Test
    void normalizeCommitKeepsRawAddressWhenUnparseable() {
        RecSyncTransaction tx = new RecSyncTransaction(
                "unknown-address-format",
                List.of(),
                List.of(),
                List.of(),
                Map.of()
        );

        assertFalse(tx.sourceAddressParsed());
        assertNotNull(tx.sourceAddress());
        assertEquals("unknown-address-format", tx.sourceAddress());
    }
}



