package org.phoebus.recceiver;

import java.util.Map;

public interface RecSyncProcessor {
    void commit(RecSyncTransaction transaction);

    default String name() {
        return getClass().getName();
    }

    default RecSyncProcessor build(String name, Map<String, String> options) {
        return this;
    }

    default void connectionClosed(ConnectionClosedEvent event) {
        // Optional callback for connection lifecycle notifications.
    }
}

