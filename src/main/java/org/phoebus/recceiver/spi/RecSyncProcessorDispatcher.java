package org.phoebus.recceiver.spi;

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
                log.log(Level.WARNING, e, () -> "Processor commit failed: " + processor.getClass().getName());
            }
        }
    }

    public void dispatchConnectionClosed(ConnectionClosedEvent event) {
        for (RecSyncProcessor processor : processors) {
            try {
                processor.connectionClosed(event);
            } catch (Exception e) {
                log.log(Level.WARNING, e, () -> "Processor connectionClosed failed: " + processor.getClass().getName());
            }
        }
    }

    /**
     * Calls destroy() on all registered processors during application shutdown.
     * This allows processors to clean up resources and perform shutdown-specific actions
     * (e.g., CFRecSyncProcessor marks IOC channels as inactive or deletes them).
     */
    public void destroyAllProcessors() {
        log.info(() -> "Destroying all " + processors.size() + " registered processors");
        for (RecSyncProcessor processor : processors) {
            try {
                processor.destroy();
            } catch (Exception e) {
                log.log(Level.WARNING, e, () -> "Processor destroy failed: " + processor.getClass().getName());
            }
        }
    }
}

