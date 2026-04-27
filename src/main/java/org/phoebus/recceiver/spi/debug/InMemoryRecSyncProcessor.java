package org.phoebus.recceiver.spi.debug;

import org.phoebus.recceiver.spi.AddRecordMessage;
import org.phoebus.recceiver.spi.ConnectionClosedEvent;
import org.phoebus.recceiver.spi.RecSyncProcessor;
import org.phoebus.recceiver.spi.RecSyncTransaction;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * This version focuses on the core in-memory behavior from cfstore.py:
 * - track IOC identity and channel membership
 * - track record aliases
 * - apply add/delete deltas per commit
 * - clean IOC state on connection close
 *
 * This is a processor primarily for debugging.
 */
public class InMemoryRecSyncProcessor implements RecSyncProcessor {
    private static final Logger log = Logger.getLogger(InMemoryRecSyncProcessor.class.getName());

    private static final String IOCNAME_KEY = "IOCNAME";
    private static final String HOSTNAME_KEY = "HOSTNAME";
    private static final String ENGINEER_KEY = "ENGINEER";
    private static final String CF_USERNAME_KEY = "CF_USERNAME";

    private static final String DEFAULT_OWNER = "cfstore";

    // channel name -> IOC id currently serving it (each channel can only be served by one IOC)
    private final Map<String, String> channelIndex = new LinkedHashMap<>();
    // IOC id -> state
    private final Map<String, IocState> iocs = new LinkedHashMap<>();

    @Override
    public synchronized void commit(RecSyncTransaction transaction) {
        Source source = Source.from(transaction);
        String iocId = source.iocId();

        IocState ioc = iocs.computeIfAbsent(iocId, ignored -> new IocState(iocId, source.host(), source.port()));
        ioc.iocName = transaction.clientInfos().getOrDefault(IOCNAME_KEY, String.valueOf(source.port()));
        ioc.hostName = transaction.clientInfos().getOrDefault(HOSTNAME_KEY, source.host());
        ioc.owner = transaction.clientInfos().getOrDefault(
                ENGINEER_KEY,
                transaction.clientInfos().getOrDefault(CF_USERNAME_KEY, DEFAULT_OWNER)
        );
        ioc.connected = transaction.connected();

        // Apply adds first so alias references can resolve in the same commit.
        for (Map.Entry<Integer, AddRecordMessage> entry : transaction.recordsToAdd().entrySet()) {
            int recId = entry.getKey();
            AddRecordMessage add = entry.getValue();

            String previousName = ioc.recordNamesById.put(recId, add.recordName());
            if (previousName != null && !previousName.equals(add.recordName())) {
                removeChannel(previousName, iocId);
            }
            addChannel(add.recordName(), iocId);
        }

        for (Map.Entry<Integer, List<String>> aliasEntry : transaction.aliases().entrySet()) {
            int recId = aliasEntry.getKey();
            List<String> aliases = aliasEntry.getValue();

            List<String> oldAliases = ioc.aliasesByRecId.get(recId);
            if (oldAliases != null) {
                for (String oldAlias : oldAliases) {
                    removeChannel(oldAlias, iocId);
                }
            }

            List<String> normalizedAliases = List.copyOf(aliases);
            ioc.aliasesByRecId.put(recId, normalizedAliases);
            for (String alias : normalizedAliases) {
                addChannel(alias, iocId);
            }
        }

        // Deletes are by record id in current Java model.
        for (Integer recId : transaction.recordsToDelete()) {
            String recordName = ioc.recordNamesById.remove(recId);
            if (recordName != null) {
                removeChannel(recordName, iocId);
            }

            List<String> aliases = ioc.aliasesByRecId.remove(recId);
            if (aliases != null) {
                for (String alias : aliases) {
                    removeChannel(alias, iocId);
                }
            }
        }

        if (!transaction.connected()) {
            disconnectIoc(iocId, "commit connected=false");
        }

        log.fine("[InMemoryRecSyncProcessor] commit source=" + transaction.sourceAddress()
                + " iocId=" + iocId
                + " initial=" + transaction.initial()
                + " adds=" + transaction.recordsToAdd().size()
                + " aliases=" + transaction.aliases().size()
                + " deletes=" + transaction.recordsToDelete().size());
    }

    @Override
    public synchronized void connectionClosed(ConnectionClosedEvent event) {
        Source source = Source.from(event.sourceAddress());
        disconnectIoc(source.iocId(), event.reason());
    }

    public synchronized int iocCount() {
        return iocs.size();
    }

    public synchronized Map<String, Integer> iocChannelCountsView() {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (Map.Entry<String, IocState> entry : iocs.entrySet()) {
            counts.put(entry.getKey(), entry.getValue().channelCount);
        }
        return Map.copyOf(counts);
    }

    public synchronized Map<String, String> channelIndexView() {
        return Map.copyOf(channelIndex);
    }

    private void addChannel(String channelName, String iocId) {
        String currentOwner = channelIndex.get(channelName);
        if (currentOwner != null && !currentOwner.equals(iocId)) {
            // Channel is currently served by a different IOC; remove it from the old one first
            removeChannel(channelName, currentOwner);
        }
        channelIndex.put(channelName, iocId);
        IocState state = iocs.get(iocId);
        if (state != null) {
            state.channelCount++;
        }
    }

    private void removeChannel(String channelName, String iocId) {
        String currentOwner = channelIndex.get(channelName);
        if (currentOwner == null || !currentOwner.equals(iocId)) {
            return;
        }
        channelIndex.remove(channelName);
        IocState state = iocs.get(iocId);
        if (state != null && state.channelCount > 0) {
            state.channelCount--;
        }
    }


    private void disconnectIoc(String iocId, String reason) {
        IocState ioc = iocs.remove(iocId);
        if (ioc == null) {
            return;
        }

        for (String channelName : ioc.recordNamesById.values()) {
            removeChannel(channelName, iocId);
        }
        for (List<String> aliases : ioc.aliasesByRecId.values()) {
            for (String alias : aliases) {
                removeChannel(alias, iocId);
            }
        }

        log.info("[InMemoryRecSyncProcessor] disconnected iocId=" + iocId + " reason='" + reason + "'");
    }

    private static class IocState {
        private final String iocId;
        private final String host;
        private final int port;

        private String iocName;
        private String hostName;
        private String owner = DEFAULT_OWNER;
        private boolean connected = true;

        private final Map<Integer, String> recordNamesById = new LinkedHashMap<>();
        private final Map<Integer, List<String>> aliasesByRecId = new LinkedHashMap<>();
        private int channelCount;

        private IocState(String iocId, String host, int port) {
            this.iocId = iocId;
            this.host = host;
            this.port = port;
        }
    }

    private record Source(String raw, String host, int port) {
        private String iocId() {
            return host + ":" + port;
        }

        private static Source from(RecSyncTransaction transaction) {
            String host = transaction.sourceHost() == null ? "unknown" : transaction.sourceHost();
            int port = transaction.sourcePort() == null ? -1 : transaction.sourcePort();
            return new Source(transaction.sourceAddress(), host, port);
        }

        private static Source from(String rawSourceAddress) {
            if (rawSourceAddress == null || rawSourceAddress.isBlank()) {
                return new Source("unknown", "unknown", -1);
            }

            String candidate = rawSourceAddress.trim();
            if (candidate.startsWith("/")) {
                candidate = candidate.substring(1);
            }

            int colon = candidate.lastIndexOf(':');
            if (colon <= 0 || colon >= candidate.length() - 1) {
                return new Source(rawSourceAddress, "unknown", -1);
            }

            String host = candidate.substring(0, colon);
            int slash = host.lastIndexOf('/');
            if (slash >= 0 && slash < host.length() - 1) {
                host = host.substring(slash + 1);
            }

            try {
                int port = Integer.parseInt(candidate.substring(colon + 1));
                return new Source(rawSourceAddress, host, port);
            } catch (NumberFormatException ignored) {
                return new Source(rawSourceAddress, "unknown", -1);
            }
        }
    }
}

