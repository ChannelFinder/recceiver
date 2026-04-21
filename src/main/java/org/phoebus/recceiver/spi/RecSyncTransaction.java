package org.phoebus.recceiver.spi;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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

    public RecSyncTransaction(
            String sourceAddress,
            List<AddRecordMessage> recordsToAdd,
            List<DelRecordMessage> recordsToDelete,
            List<AddInfoMessage> infoMessages,
            Map<String, String> clientInfos
    ) {
        this(sourceAddress, recordsToAdd, recordsToDelete, infoMessages, clientInfos, true, true);
    }

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

    public String sourceAddress() {
        return sourceAddress;
    }

    public String sourceHost() {
        return sourceHost;
    }

    public Integer sourcePort() {
        return sourcePort;
    }

    public boolean sourceAddressParsed() {
        return sourceAddressParsed;
    }

    public Map<Integer, AddRecordMessage> recordsToAdd() {
        return recordsToAdd;
    }

    public Set<Integer> recordsToDelete() {
        return recordsToDelete;
    }

    public Map<String, String> clientInfos() {
        return clientInfos;
    }

    public Map<Integer, Map<String, String>> recordInfosToAdd() {
        return recordInfosToAdd;
    }

    public Map<Integer, List<String>> aliases() {
        return aliases;
    }

    public boolean initial() {
        return initial;
    }

    public boolean connected() {
        return connected;
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


    private static Map<Integer, AddRecordMessage> buildRecordsToAdd(List<AddRecordMessage> addMessages) {
        Map<Integer, AddRecordMessage> map = new LinkedHashMap<>();
        for (AddRecordMessage add : addMessages) {
            if (add.aType() != 1) {
                map.put(add.recId(), add);
            }
        }
        return map;
    }

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
}

