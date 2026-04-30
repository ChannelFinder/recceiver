package org.phoebus.recceiver.spi;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable container for a channel update transaction from an IOC via the RecSync protocol.
 *
 * <p><strong>Source:</strong> The IOC's socket address and parsed host/port (if available).</p>
 *
 * <p><strong>Channel Updates:</strong>
 * <ul>
 *   <li>{@link #recordsToAdd()}: Map of recId → primary record. These are actual control system
 *       records published by the IOC.</li>
 *   <li>{@link #recordsToDelete()}: Set of recIds that the IOC has deleted. Delete messages carry only
 *       the recId; processors must track record names from prior adds to resolve which channels to delete.</li>
 *   <li>{@link #aliases()}: Map of recId → list of alias names. Each alias is an alternate name for
 *       the primary record (same recId, aType=1 in the wire format).</li>
 *   <li>{@link #recordInfosToAdd()}: Map of recId → property map (metadata like "PREC", "SCAN", etc.).
 *       These are optional per-record key-value pairs.</li>
 * </ul>
 * </p>
 *
 * <p><strong>Client Metadata:</strong>
 * <ul>
 *   <li>{@link #clientInfos()}: Global key-value pairs sent by the IOC. Typical keys include "IOCNAME",
 *       "HOSTNAME", "ENGINEER", environment variable mappings, and custom application data.</li>
 * </ul>
 * </p>
 *
 * <p><strong>Lifecycle Flags:</strong>
 * <ul>
 *   <li>{@link #initial()}: {@code true} on the IOC's first commit after connection. Signals a complete
 *       initial channel list. {@code false} for incremental updates in steady state.</li>
 *   <li>{@link #connected()}: {@code true} if the IOC is online and connected. {@code false} if the IOC
 *       gracefully disconnected, signaling immediate unavailability. (Most disconnections are detected
 *       via {@link ConnectionClosedEvent} in the {@code connectionClosed()} callback instead.)</li>
 * </ul>
 * </p>
 *
 * <p>All fields are immutable. The transaction is safe to share across threads.</p>
 */
public final class RecSyncTransaction {
    private final String sourceAddress;
    private final String sourceHost;
    private final Integer sourcePort;
    private final boolean sourceAddressParsed;
    private final Map<Integer, AddRecordMessage> recordsToAdd;
    private final Set<Integer> recordsToDelete;
    private final Map<String, String> clientInfos;
    private final Map<Integer, Map<String, String>> recordInfosToAdd;
    private final Map<Integer, List<String>> aliases;
    private final boolean initial;
    private final boolean connected;

    /**
     * Creates a transaction with default initial=true and connected=true flags.
     *
     * @param sourceAddress the IOC's socket address (e.g., "/127.0.0.1:5070").
     * @param recordsToAdd list of primary records to add.
     * @param recordsToDelete list of record IDs to delete.
     * @param infoMessages list of metadata key-value pairs.
     * @param clientInfos global client metadata (IOCNAME, HOSTNAME, environment variables, etc.).
     */
    public RecSyncTransaction(
            String sourceAddress,
            List<AddRecordMessage> recordsToAdd,
            List<DelRecordMessage> recordsToDelete,
            List<AddInfoMessage> infoMessages,
            Map<String, String> clientInfos
    ) {
        this(sourceAddress, recordsToAdd, recordsToDelete, infoMessages, clientInfos, true, true);
    }

    /**
     * Creates a transaction with explicit initial and connected flags.
     *
     * @param sourceAddress the IOC's socket address (e.g., "/127.0.0.1:5070").
     * @param recordsToAdd list of primary records to add.
     * @param recordsToDelete list of record IDs to delete.
     * @param infoMessages list of metadata key-value pairs.
     * @param clientInfos global client metadata.
     * @param initial {@code true} if this is the IOC's first commit after connection, signaling a
     *                complete channel list. {@code false} for incremental updates.
     * @param connected {@code true} if the IOC is online. {@code false} if the IOC gracefully
     *                  disconnected.
     */
    public RecSyncTransaction(
            String sourceAddress,
            List<AddRecordMessage> recordsToAdd,
            List<DelRecordMessage> recordsToDelete,
            List<AddInfoMessage> infoMessages,
            Map<String, String> clientInfos,
            boolean initial,
            boolean connected
    ) {
        this(
                sourceAddress,
                parseSource(sourceAddress),
                buildRecordsToAdd(recordsToAdd),
                buildRecordsToDelete(recordsToDelete),
                Map.copyOf(clientInfos),
                buildRecordInfosToAdd(infoMessages),
                buildAliases(recordsToAdd),
                initial,
                connected
        );
    }

    private RecSyncTransaction(
            String sourceAddress,
            ParsedSource parsed,
            Map<Integer, AddRecordMessage> recordsToAdd,
            Set<Integer> recordsToDelete,
            Map<String, String> clientInfos,
            Map<Integer, Map<String, String>> recordInfosToAdd,
            Map<Integer, List<String>> aliases,
            boolean initial,
            boolean connected
    ) {
        this.sourceAddress = sourceAddress;
        this.sourceHost = parsed.host;
        this.sourcePort = parsed.port;
        this.sourceAddressParsed = parsed.parsed;
        this.recordsToAdd = Map.copyOf(recordsToAdd);
        this.recordsToDelete = Set.copyOf(recordsToDelete);
        this.clientInfos = Map.copyOf(clientInfos);
        this.recordInfosToAdd = recordInfosToAdd.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                e -> Map.copyOf(e.getValue())
        ));
        this.aliases = aliases.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                e -> List.copyOf(e.getValue())
        ));
        this.initial = initial;
        this.connected = connected;
    }

    /**
     * Returns the IOC's socket address as reported by the OS.
     *
     * <p>Format is typically "/host:port" or "host:port" (implementation-dependent). May be used
     * directly as an IOC identifier if {@link #sourceAddressParsed()} is false (parsing failed).</p>
     *
     * @return the socket address string.
     */
    public String sourceAddress() {
        return sourceAddress;
    }

    /**
     * Returns the parsed host name from {@link #sourceAddress()}, or {@code null} if parsing failed.
     *
     * @return the host name (lowercase), or {@code null}.
     */
    public String sourceHost() {
        return sourceHost;
    }

    /**
     * Returns the parsed port number from {@link #sourceAddress()}, or {@code null} if parsing failed.
     *
     * @return the port number (0–65535), or {@code null}.
     */
    public Integer sourcePort() {
        return sourcePort;
    }

    /**
     * Returns whether the source address was successfully parsed into host and port components.
     *
     * @return {@code true} if host and port are available; {@code false} otherwise.
     */
    public boolean sourceAddressParsed() {
        return sourceAddressParsed;
    }

    /**
     * Returns the map of primary records to add, keyed by recId.
     *
     * <p>These are actual control system records (aType=0 in the wire format). Aliases (aType=1) are
     * excluded from this map and returned separately via {@link #aliases()}. Processors typically
     * create a channel in their target system for each primary record.</p>
     *
     * @return immutable map of recId → AddRecordMessage. Empty if no records to add.
     */
    public Map<Integer, AddRecordMessage> recordsToAdd() {
        return recordsToAdd;
    }

    /**
     * Returns the set of record IDs to delete.
     *
     * <p><strong>Important:</strong> Delete messages in the RecSync protocol carry only the recId,
     * not the channel name. Processors must track the mapping from recId to channel names (both
     * primary records and aliases) from prior {@link #recordsToAdd()} commits in order to delete
     * the correct channels. The {@link #aliases()} method helps with this: for each deleted recId,
     * look up both the primary name (from prior recordsToAdd) and any aliases in the prior transaction.</p>
     *
     * @return immutable set of recIds to delete. Empty if no records to delete.
     */
    public Set<Integer> recordsToDelete() {
        return recordsToDelete;
    }

    /**
     * Returns the global client metadata (environment variables and application data).
     *
     * <p>Typical keys include "IOCNAME", "HOSTNAME", "ENGINEER", "CF_USERNAME", "RSRV_SERVER_PORT",
     * "PVAS_SERVER_PORT", and custom application-defined keys. This is the same across all records
     * in a single transaction.</p>
     *
     * @return immutable map of key → value. Empty if no client info provided.
     */
    public Map<String, String> clientInfos() {
        return clientInfos;
    }

    /**
     * Returns per-record metadata added in this transaction, keyed by recId.
     *
     * <p>Each entry maps to a key-value map.
     * Only populated for records in {@link #recordsToAdd()}. Info for deleted records is not retained.</p>
     *
     * @return immutable map of recId → (property name → value). Empty if no info to add.
     */
    public Map<Integer, Map<String, String>> recordInfosToAdd() {
        return recordInfosToAdd;
    }

    /**
     * Returns the alias names for each primary record, keyed by recId.
     *
     * <p>Aliases are alternate names for the same record (aType=1 in the wire format). Each alias
     * shares the same recId and record type as the primary record. Processors typically create a
     * separate channel for each alias with a reference back to the primary record name.</p>
     *
     * <p>When resolving deletes: if recId X is deleted and {@link #aliases()} contains an entry
     * for X, both the primary record and all aliases must be deleted.</p>
     *
     * @return immutable map of recId → list of alias names. Empty if no aliases.
     */
    public Map<Integer, List<String>> aliases() {
        return aliases;
    }

    /**
     * Returns whether this is the IOC's first commit after connection.
     *
     * <p>{@code true} signals that this transaction contains the complete initial channel list.
     * Processors often perform special handling on initial commits (e.g., full refresh of channels
     * for this IOC). {@code false} indicates an incremental update in steady state.</p>
     *
     * @return {@code true} for first commit, {@code false} for incremental updates.
     */
    public boolean initial() {
        return initial;
    }

    /**
     * Returns the connection state of the IOC.
     *
     * <p>{@code true} if the IOC is online and connected. {@code false} if the IOC gracefully
     * disconnected (rare in practice; most disconnections are detected via
     * {@link RecSyncProcessor#connectionClosed(ConnectionClosedEvent)} instead).</p>
     *
     * <p>When {@code false}, processors typically treat the IOC as immediately unavailable and
     * clean up its channels (delete or mark inactive).</p>
     *
     * @return {@code true} if connected, {@code false} if gracefully disconnected.
     */
    public boolean connected() {
        return connected;
    }

    private static Map<Integer, AddRecordMessage> buildRecordsToAdd(List<AddRecordMessage> addMessages) {
        Map<Integer, AddRecordMessage> map = new LinkedHashMap<>();
        for (AddRecordMessage add : addMessages) {
            if (add.aType() != 1) {
                map.put(add.recId(), add);
            }
        }
        return map;
    }

    /**
     * Extracts aliases from the raw add messages.
     *
     * <p>Messages with aType=1 are aliases: they share a recId with a primary record but have a
     * different name. This method groups them by recId for easy lookup during delete resolution.</p>
     */
    private static Map<Integer, List<String>> buildAliases(List<AddRecordMessage> addMessages) {
        Map<Integer, List<String>> map = new LinkedHashMap<>();
        for (AddRecordMessage add : addMessages) {
            if (add.aType() == 1) {
                map.computeIfAbsent(add.recId(), key -> new ArrayList<>()).add(add.recordName());
            }
        }
        return map;
    }

    private static Map<Integer, Map<String, String>> buildRecordInfosToAdd(List<AddInfoMessage> infoMessages) {
        Map<Integer, Map<String, String>> map = new LinkedHashMap<>();
        for (AddInfoMessage info : infoMessages) {
            if (info.recId() > 0) {
                map.computeIfAbsent(info.recId(), key -> new LinkedHashMap<>())
                        .put(info.key(), info.value());
            }
        }
        return map;
    }

    private static Set<Integer> buildRecordsToDelete(List<DelRecordMessage> deleteMessages) {
        Set<Integer> set = new LinkedHashSet<>();
        for (DelRecordMessage del : deleteMessages) {
            set.add(del.recId());
        }
        return set;
    }

    /**
     * Parses the source address string into host and port components.
     *
     * <p>Input format is typically "/host:port" or "host:port". Returns a ParsedSource with
     * host=null and port=null if parsing fails or the address is invalid.</p>
     */
    private static ParsedSource parseSource(String sourceAddress) {
        if (sourceAddress == null || sourceAddress.isBlank()) {
            return new ParsedSource(null, null, false);
        }

        String candidate = sourceAddress.trim();
        if (candidate.startsWith("/")) {
            candidate = candidate.substring(1);
        }

        int colonIndex = candidate.lastIndexOf(':');
        if (colonIndex <= 0 || colonIndex >= candidate.length() - 1) {
            return new ParsedSource(null, null, false);
        }

        String hostPart = candidate.substring(0, colonIndex);
        String portPart = candidate.substring(colonIndex + 1);

        try {
            int port = Integer.parseInt(portPart);
            if (port < 0 || port > 65535) {
                return new ParsedSource(null, null, false);
            }
            return new ParsedSource(normalizeHost(hostPart), port, true);
        } catch (NumberFormatException ignored) {
            return new ParsedSource(null, null, false);
        }
    }

    private static String normalizeHost(String hostPart) {
        String host = hostPart;
        int slashIndex = hostPart.lastIndexOf('/');
        if (slashIndex >= 0 && slashIndex < hostPart.length() - 1) {
            host = hostPart.substring(slashIndex + 1);
        }
        if (host.isBlank()) {
            return null;
        }
        return host.toLowerCase(Locale.ROOT);
    }

    private record ParsedSource(String host, Integer port, boolean parsed) {
    }

    @Override
    public String toString() {
        return "RecSyncTransaction{" +
                "sourceAddress='" + sourceAddress + '\'' +
                ", sourceHost='" + sourceHost + '\'' +
                ", sourcePort=" + sourcePort +
                ", sourceAddressParsed=" + sourceAddressParsed +
                ", recordsToAdd=" + recordsToAdd +
                ", recordsToDelete=" + recordsToDelete +
                ", clientInfos=" + clientInfos +
                ", recordInfosToAdd=" + recordInfosToAdd +
                ", aliases=" + aliases +
                ", initial=" + initial +
                ", connected=" + connected +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecSyncTransaction that = (RecSyncTransaction) o;
        return sourceAddressParsed == that.sourceAddressParsed
                && initial == that.initial
                && connected == that.connected
                && Objects.equals(sourceAddress, that.sourceAddress)
                && Objects.equals(sourceHost, that.sourceHost)
                && Objects.equals(sourcePort, that.sourcePort)
                && Objects.equals(recordsToAdd, that.recordsToAdd)
                && Objects.equals(recordsToDelete, that.recordsToDelete)
                && Objects.equals(clientInfos, that.clientInfos)
                && Objects.equals(recordInfosToAdd, that.recordInfosToAdd)
                && Objects.equals(aliases, that.aliases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                sourceAddress,
                sourceHost,
                sourcePort,
                sourceAddressParsed,
                recordsToAdd,
                recordsToDelete,
                clientInfos,
                recordInfosToAdd,
                aliases,
                initial,
                connected
        );
    }
}

