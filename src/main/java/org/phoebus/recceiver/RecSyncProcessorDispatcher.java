package org.phoebus.recceiver;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RecSyncProcessorDispatcher {
    private static final Logger log = Logger.getLogger(RecSyncProcessorDispatcher.class.getName());

    private final List<RecSyncProcessor> processors;

    public RecSyncProcessorDispatcher(List<RecSyncProcessor> processors) {
        this.processors = List.copyOf(processors);
    }

    public static RecSyncProcessorDispatcher empty() {
        return new RecSyncProcessorDispatcher(List.of());
    }

    public int size() {
        return processors.size();
    }

    public void dispatchCommit(RecSyncTransaction transaction) {
        for (RecSyncProcessor processor : processors) {
            try {
                processor.commit(transaction);
            } catch (Exception e) {
                log.log(Level.WARNING, "Processor commit failed: " + processor.getClass().getName(), e);
            }
        }
    }

    public void dispatchConnectionClosed(ConnectionClosedEvent event) {
        for (RecSyncProcessor processor : processors) {
            try {
                processor.connectionClosed(event);
            } catch (Exception e) {
                log.log(Level.WARNING, "Processor connectionClosed failed: " + processor.getClass().getName(), e);
            }
        }
    }
}

