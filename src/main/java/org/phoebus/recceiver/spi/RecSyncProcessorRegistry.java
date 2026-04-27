package org.phoebus.recceiver.spi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class RecSyncProcessorRegistry {
    private static final Logger log = Logger.getLogger(RecSyncProcessorRegistry.class.getName());

    private RecSyncProcessorRegistry() {
    }

    public static RecSyncProcessorDispatcher loadDispatcher() {
        List<RecSyncProcessor> processors = new ArrayList<>();

        ServiceLoader<RecSyncProcessor> directLoader = ServiceLoader.load(RecSyncProcessor.class);
        for (RecSyncProcessor processor : directLoader) {
            try {
                RecSyncProcessor initializedProcessor = processor.init(processor.name(), Map.of());
                if (initializedProcessor != null) {
                    processors.add(initializedProcessor);
                    log.info("Loaded RecSyncProcessor via SPI: " + initializedProcessor.getClass().getName());
                }
            } catch (Exception e) {
                log.log(Level.WARNING, "Failed to initialize processor " + processor.getClass().getName(), e);
            }
        }

        log.info("RecSync SPI processors loaded: " + processors.size());
        return new RecSyncProcessorDispatcher(processors);
    }
}

