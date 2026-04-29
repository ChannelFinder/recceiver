package org.phoebus.recceiver.spi;

import java.util.Map;

public interface RecSyncProcessor {

    default RecSyncProcessor init(String name, Map<String, String> options) {
        return this;
    }

    default String name() {
        return getClass().getName();
    }

    void commit(RecSyncTransaction transaction);

    default void connectionClosed(ConnectionClosedEvent event) {
        // Optional callback for connection lifecycle notifications.
    }

    default void destroy() {
        // Optional callback for cleanup when the processor is being removed.
    }
}

