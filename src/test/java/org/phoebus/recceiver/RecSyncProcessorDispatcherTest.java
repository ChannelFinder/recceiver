package org.phoebus.recceiver;

import org.junit.jupiter.api.Test;
import org.phoebus.recceiver.spi.ConnectionClosedEvent;
import org.phoebus.recceiver.spi.RecSyncProcessor;
import org.phoebus.recceiver.spi.RecSyncProcessorDispatcher;
import org.phoebus.recceiver.spi.RecSyncTransaction;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RecSyncProcessorDispatcherTest {

    @Test
    void dispatchCommitContinuesAfterProcessorFailure() {
        CapturingProcessor capturing = new CapturingProcessor();
        FailingProcessor failing = new FailingProcessor();
        RecSyncProcessorDispatcher dispatcher = new RecSyncProcessorDispatcher(List.of(failing, capturing));

        RecSyncTransaction transaction = new RecSyncTransaction("127.0.0.1", List.of(), List.of(), List.of(), java.util.Map.of());
        dispatcher.dispatchCommit(transaction);

        assertEquals(1, capturing.commits.size());
        assertEquals("127.0.0.1", capturing.commits.getFirst().sourceAddress());
    }

    @Test
    void dispatchConnectionClosedContinuesAfterProcessorFailure() {
        CapturingProcessor capturing = new CapturingProcessor();
        FailingProcessor failing = new FailingProcessor();
        RecSyncProcessorDispatcher dispatcher = new RecSyncProcessorDispatcher(List.of(failing, capturing));

        dispatcher.dispatchConnectionClosed(new ConnectionClosedEvent("127.0.0.1", "eof"));

        assertEquals(1, capturing.closedEvents.size());
        assertEquals("eof", capturing.closedEvents.getFirst().reason());
    }

    @Test
    void destroyAllProcessorsContinuesAfterProcessorFailure() {
        CapturingProcessor capturing = new CapturingProcessor();
        FailingProcessor failing = new FailingProcessor();
        RecSyncProcessorDispatcher dispatcher = new RecSyncProcessorDispatcher(List.of(failing, capturing));

        dispatcher.destroyAllProcessors();

        assertEquals(1, capturing.destroyCalls);
    }

    private static class CapturingProcessor implements RecSyncProcessor {
        private final List<RecSyncTransaction> commits = new ArrayList<>();
        private final List<ConnectionClosedEvent> closedEvents = new ArrayList<>();
        private int destroyCalls;

        @Override
        public void commit(RecSyncTransaction transaction) {
            commits.add(transaction);
        }

        @Override
        public void connectionClosed(ConnectionClosedEvent event) {
            closedEvents.add(event);
        }

        @Override
        public void destroy() {
            destroyCalls++;
        }
    }

    private static class FailingProcessor implements RecSyncProcessor {
        @Override
        public void commit(RecSyncTransaction transaction) {
            throw new RuntimeException("boom");
        }

        @Override
        public void connectionClosed(ConnectionClosedEvent event) {
            throw new RuntimeException("boom");
        }

        @Override
        public void destroy() {
            throw new RuntimeException("boom");
        }
    }
}
