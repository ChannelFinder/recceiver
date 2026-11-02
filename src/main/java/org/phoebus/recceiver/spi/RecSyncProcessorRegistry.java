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

    public static RecSyncProcessorDispatcher loadDispatcher(Map<String, String> options) {
        List<RecSyncProcessor> processors = new ArrayList<>();
        Map<String, String> initOptions = options == null ? Map.of() : Map.copyOf(options);

        ServiceLoader<RecSyncProcessor> directLoader = ServiceLoader.load(RecSyncProcessor.class);
        for (RecSyncProcessor processor : directLoader) {
            log.info("Loading RecSyncProcessor via SPI: " + processor.getClass().getName());
            try {
                RecSyncProcessor initializedProcessor = processor.init(processor.name(), initOptions);
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
