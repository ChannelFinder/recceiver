package org.phoebus.recceiver.spi;

import java.util.Map;

/**
 * SPI contract for RecSync protocol message processors.
 *
 * <p>Implementations of this interface receive control callbacks as IOCs connect and send
 * channel records to the recceiver. The lifecycle of a processor is:</p>
 *
 * <ol>
 *   <li><strong>Instantiation:</strong> Service loader creates an instance with a no-arg constructor.</li>
 *   <li><strong>Initialization ({@link #init(String, Map)}):</strong> Called once before any transaction.
 *       Options include configuration properties from application.properties and Spring @Value injection.</li>
 *   <li><strong>Commits ({@link #commit(RecSyncTransaction)}):</strong> Called when an IOC sends channel
 *       updates (add records, delete records, infos). The transaction may be marked as {@code initial=true}
 *       on first connection and {@code connected=false} on graceful disconnect.</li>
 *   <li><strong>Connection Loss ({@link #connectionClosed(ConnectionClosedEvent)}):</strong> Called when
 *       the TCP connection closes abnormally (e.g., network fault, timeout). The processor should treat
 *       this as immediate IOC unavailability and clean up associated resources as appropriate.</li>
 *   <li><strong>Shutdown ({@link #destroy()}):</strong> Called during application shutdown to clean up
 *       resources. Database connections, external service state, etc. should be finalized here.</li>
 * </ol>
 *
 * <p><strong>Concurrency:</strong> Commit calls may be concurrent for different IOCs. Implementations
 * must be thread-safe and avoid blocking operations that would stall message processing.</p>
 */
public interface RecSyncProcessor {

    /**
     * Initializes this processor with the given name and configuration options.
     *
     * <p>This is called exactly once before any transaction commits are dispatched. Implementations
     * should use this to establish external connections, validate configuration, create required
     * resources, and perform any one-time setup.</p>
     *
     * @param name the processor name (typically the class name or a configured identifier)
     * @param options configuration properties from application.properties and Spring @Value fields.
     *                 Implementations may define custom keys for their configuration (e.g.,
     *                 "recceiver.cf.service-url", "recceiver.cf.delete-on-clean").
     *
     * @return the initialized processor, or {@code null} to disable this processor (for conditional
     *         enablement based on configuration). Returns {@code this} by default.
     *
     * @throws IllegalArgumentException if required configuration is missing or invalid.
     */
    default RecSyncProcessor init(String name, Map<String, String> options) {
        return this;
    }

    /**
     * Returns the human-readable name of this processor.
     *
     * @return the processor name (e.g., "CFRecSyncProcessor"). Defaults to the class name.
     */
    default String name() {
        return getClass().getName();
    }

    /**
     * Processes a transaction containing channel updates from a connected IOC.
     *
     * <p>This is called when the IOC sends channel add/delete/info messages. The transaction contains:</p>
     *
     * <ul>
     *   <li><strong>Source address:</strong> The IOC's socket address (e.g., "/127.0.0.1:5070").</li>
     *   <li><strong>Records to add:</strong> Primary records and aliases announced by the IOC.</li>
     *   <li><strong>Records to delete:</strong> Record IDs that the IOC has deleted.</li>
     *   <li><strong>Infos to add:</strong> Optional metadata and environment variables.</li>
     *   <li><strong>Initial flag:</strong> {@code true} on the IOC's first commit after connection,
     *       signaling a complete channel list. {@code false} for incremental updates.</li>
     *   <li><strong>Connected flag:</strong> {@code true} if the IOC is online. {@code false} if the IOC
     *       gracefully disconnected (rare; most disconnections are detected via {@link #connectionClosed}).</li>
     * </ul>
     *
     * <p><strong>Note:</strong> Delete messages contain only record IDs. Processors must track record
     * names from prior adds to resolve which channels to delete.</p>
     *
     * @param transaction the transaction containing channel updates from an IOC.
     */
    void commit(RecSyncTransaction transaction);

    /**
     * Callback invoked when a TCP connection to an IOC closes unexpectedly.
     *
     * <p>This is called when the recceiver detects that the connection has been closed abnormally,
     * e.g., due to network fault, timeout, or EOF. Implementations should treat this as immediate
     * IOC unavailability and perform appropriate cleanup.</p>
     *
     * @param event the connection closed event containing the source address and close reason.
     */
    default void connectionClosed(ConnectionClosedEvent event) {
        // Optional callback for connection lifecycle notifications.
    }

    /**
     * Callback invoked during application shutdown.
     *
     * <p>This is called when the application is shutting down, typically via Spring's {@code @PreDestroy}
     * lifecycle. Implementations should finalize resources, close external connections, and clean up
     * any persisted state as needed (e.g., mark all tracked IOC channels as inactive, or delete them,
     * or flush pending updates to a database).</p>
     *
     * <p>Implementations may choose to perform different cleanup strategies based on configuration
     * (e.g., delete on shutdown vs. mark inactive), similar to how they handle connection loss.</p>
     */
    default void destroy() {
        // Optional callback for cleanup when the processor is being removed.
    }
}

