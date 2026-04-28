package org.phoebus.recceiver.spi.cf;

import org.phoebus.recceiver.spi.AddRecordMessage;
import org.phoebus.recceiver.spi.ConnectionClosedEvent;
import org.phoebus.recceiver.spi.RecSyncProcessor;
import org.phoebus.recceiver.spi.RecSyncTransaction;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Publishes primary record adds to ChannelFinder.
 */
public class CFRecSyncProcessor implements RecSyncProcessor {
    private static final Logger log = Logger.getLogger(CFRecSyncProcessor.class.getName());
    private static final String ENABLED_KEY = "recceiver.cf.enabled";
    private static final String SERVICE_URL_KEY = "recceiver.cf.service-url";
    private static final String USERNAME_KEY = "recceiver.cf.username";
    private static final String PASSWORD_KEY = "recceiver.cf.password";
    private static final String DELETE_ON_CLEAN_KEY = "recceiver.cf.delete-on-clean";
    private static final String ALIAS_ENABLED_KEY = "recceiver.cf.alias-enabled";

    // Inbound keys read from RecSync transaction client info / environment fields.
    // These names match protocol/env key spellings.
    public static final String IOCNAME_KEY = "IOCNAME";
    public static final String HOSTNAME_KEY = "HOSTNAME";
    public static final String ENGINEER_KEY = "ENGINEER";
    public static final String CF_USERNAME_KEY = "CF_USERNAME";
    public static final String CA_PORT_ENV_KEY = "RSRV_SERVER_PORT";
    public static final String PVA_PORT_ENV_KEY = "PVAS_SERVER_PORT";

    // ChannelFinder persisted property names written into channel documents.
    // These values follow ChannelFinder property names,
    // (for example: iocid, iocName, hostName).
    public static final String HOST_NAME_PROPERTY = "hostName";
    public static final String IOC_NAME_PROPERTY = "iocName";
    public static final String IOC_ID_PROPERTY = "iocid";
    public static final String IOC_IP_PROPERTY = "iocIP";
    public static final String PV_STATUS_PROPERTY = "pvStatus";
    public static final String TIME_PROPERTY = "time";
    public static final String RECCEIVER_ID_PROPERTY = "recceiverID";
    public static final String RECORD_TYPE_PROPERTY = "recordType";
    public static final String ALIAS_PROPERTY = "alias";
    public static final String CA_PORT_PROPERTY = "caPort";
    public static final String PVA_PORT_PROPERTY = "pvaPort";
    public static final String ACTIVE = "Active";
    public static final String INACTIVE = "Inactive";
    public static final String DEFAULT_OWNER = "cfstore";

    private static final String RECCEIVER_ID = resolveRecceiverId();

    private static final Set<String> REQUIRED_PROPERTIES = Set.copyOf(new LinkedHashSet<>(Set.of(
            HOST_NAME_PROPERTY,
            IOC_NAME_PROPERTY,
            PV_STATUS_PROPERTY,
            TIME_PROPERTY,
            IOC_ID_PROPERTY,
            IOC_IP_PROPERTY,
            RECCEIVER_ID_PROPERTY,
            RECORD_TYPE_PROPERTY,
            CA_PORT_PROPERTY,
            PVA_PORT_PROPERTY
    )));

    private ChannelFinderStore store;
    private boolean deleteOnClean;
    private boolean aliasEnabled;

    public CFRecSyncProcessor() {
        // Required by ServiceLoader.
    }

    @Override
    public RecSyncProcessor init(String name, Map<String, String> options) {
        if (!isEnabled(options)) {
            return null;
        }
        String serviceUrl = options.get(SERVICE_URL_KEY);
        if (serviceUrl == null || serviceUrl.isBlank()) {
            throw new IllegalArgumentException("recceiver.cf.service-url must be configured");
        }

        String username = normalized(options.get(USERNAME_KEY));
        String password = normalized(options.get(PASSWORD_KEY));
        if ((username == null) != (password == null)) {
            throw new IllegalArgumentException(
                    "recceiver.cf.username and recceiver.cf.password must both be configured when authentication is used");
        }

        this.store = createStore(serviceUrl, username, password);
        this.deleteOnClean = Boolean.parseBoolean(options.getOrDefault(DELETE_ON_CLEAN_KEY, "false"));
        this.aliasEnabled = Boolean.parseBoolean(options.getOrDefault(ALIAS_ENABLED_KEY, "false"));
        String propertyOwner = username == null ? DEFAULT_OWNER : username;
        Set<String> requiredProperties = new LinkedHashSet<>(REQUIRED_PROPERTIES);
        if (aliasEnabled) {
            requiredProperties.add(ALIAS_PROPERTY);
        }
        this.store.ensureProperties(requiredProperties, propertyOwner);
        return this;
    }

    protected ChannelFinderStore createStore(String serviceUrl, String username, String password) {
        return username == null
                ? new ChannelFinderHttpStore(serviceUrl)
                : new ChannelFinderHttpStore(serviceUrl, username, password);
    }

    @Override
    public void commit(RecSyncTransaction transaction) {
        log.info(() -> "[CFRecSyncProcessor] commit source=" + transaction.sourceAddress()
                + " initial=" + transaction.initial()
                + " connected=" + transaction.connected()
                + " adds=" + transaction.recordsToAdd().size()
                + " aliases=" + transaction.aliases().size()
                + " deletes=" + transaction.recordsToDelete().size()
                + " recordInfos=" + transaction.recordInfosToAdd().size()
                + " clientInfos=" + transaction.clientInfos().size());

        Collection<ChannelFinderChannel> channels = buildChannels(transaction);
        if (!channels.isEmpty()) {
            store.updateChannels(channels);
        }
    }

    private Collection<ChannelFinderChannel> buildChannels(RecSyncTransaction transaction) {
        Collection<ChannelFinderChannel> channels = new ArrayList<>();
        Map<String, String> clientInfos = transaction.clientInfos();
        String owner = resolveOwner(clientInfos);
        String hostName = resolveHostName(transaction, clientInfos);
        String iocName = resolveIocName(transaction, clientInfos);
        String iocId = resolveIocId(transaction);
        String iocIp = resolveIocIp(transaction);
        String timestamp = OffsetDateTime.now().toString();
        String caPort = normalized(clientInfos.get(CA_PORT_ENV_KEY));
        String pvaPort = normalized(clientInfos.get(PVA_PORT_ENV_KEY));

        for (AddRecordMessage addRecord : transaction.recordsToAdd().values()) {
            if (addRecord.aType() != 0) {
                continue;
            }
            Map<String, String> properties = buildStandardProperties(
                    addRecord,
                    hostName,
                    iocName,
                    iocId,
                    iocIp,
                    timestamp,
                    caPort,
                    pvaPort
            );
            channels.add(new ChannelFinderChannel(addRecord.recordName(), owner, properties));
        }

        if (aliasEnabled) {
            for (Map.Entry<Integer, List<String>> aliasEntry : transaction.aliases().entrySet()) {
                AddRecordMessage primaryRecord = transaction.recordsToAdd().get(aliasEntry.getKey());
                if (primaryRecord == null) {
                    log.warning(() -> "[CFRecSyncProcessor] alias recId " + aliasEntry.getKey()
                            + " has no primary record in recordsToAdd");
                    continue;
                }
                for (String aliasName : aliasEntry.getValue()) {
                    if (aliasName == null || aliasName.isBlank()) {
                        continue;
                    }
                    Map<String, String> aliasProperties = buildStandardProperties(
                            primaryRecord,
                            hostName,
                            iocName,
                            iocId,
                            iocIp,
                            timestamp,
                            caPort,
                            pvaPort
                    );
                    aliasProperties.put(ALIAS_PROPERTY, primaryRecord.recordName());
                    channels.add(new ChannelFinderChannel(aliasName, owner, aliasProperties));
                }
            }
        }

        return channels;
    }

    private static String resolveHostName(RecSyncTransaction transaction, Map<String, String> clientInfos) {
        String hostName = normalized(clientInfos.get(HOSTNAME_KEY));
        return hostName != null ? hostName : fallbackSourceHost(transaction);
    }

    private static String resolveIocName(RecSyncTransaction transaction, Map<String, String> clientInfos) {
        String iocName = normalized(clientInfos.get(IOCNAME_KEY));
        if (iocName != null) {
            return iocName;
        }
        return transaction.sourcePort() != null ? String.valueOf(transaction.sourcePort()) : transaction.sourceAddress();
    }

    private static String resolveIocId(RecSyncTransaction transaction) {
        return transaction.sourceHost() != null && transaction.sourcePort() != null
                ? transaction.sourceHost() + ":" + transaction.sourcePort()
                : transaction.sourceAddress();
    }

    private static String resolveIocIp(RecSyncTransaction transaction) {
        return fallbackSourceHost(transaction);
    }

    private static String fallbackSourceHost(RecSyncTransaction transaction) {
        return transaction.sourceHost() != null ? transaction.sourceHost() : transaction.sourceAddress();
    }

    private static Map<String, String> buildStandardProperties(
            AddRecordMessage addRecord,
            String hostName,
            String iocName,
            String iocId,
            String iocIp,
            String timestamp,
            String caPort,
            String pvaPort
    ) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(HOST_NAME_PROPERTY, hostName);
        properties.put(IOC_NAME_PROPERTY, iocName);
        properties.put(PV_STATUS_PROPERTY, ACTIVE);
        properties.put(TIME_PROPERTY, timestamp);
        properties.put(IOC_ID_PROPERTY, iocId);
        properties.put(IOC_IP_PROPERTY, iocIp);
        properties.put(RECCEIVER_ID_PROPERTY, RECCEIVER_ID);
        properties.put(RECORD_TYPE_PROPERTY, addRecord.recordType());
        if (caPort != null) {
            properties.put(CA_PORT_PROPERTY, caPort);
        }
        if (pvaPort != null) {
            properties.put(PVA_PORT_PROPERTY, pvaPort);
        }
        return properties;
    }

    @Override
    public void connectionClosed(ConnectionClosedEvent event) {
        log.info(() -> "[CFRecSyncProcessor] connectionClosed source=" + event.sourceAddress()
                + " reason='" + event.reason() + "'");

        String iocId = resolveIocId(event.sourceAddress());
        if (iocId == null || iocId.isBlank()) {
            return;
        }

        Map<String, ChannelFinderChannel> existing = store.findByProperty(IOC_ID_PROPERTY, iocId);
        if (existing.isEmpty()) {
            return;
        }

        if (deleteOnClean) {
            store.deleteChannels(existing.keySet());
            return;
        }

        String now = OffsetDateTime.now().toString();
        Collection<ChannelFinderChannel> inactiveChannels = new ArrayList<>();
        for (ChannelFinderChannel channel : existing.values()) {
            Map<String, String> properties = new LinkedHashMap<>(channel.properties());
            properties.put(PV_STATUS_PROPERTY, INACTIVE);
            properties.put(TIME_PROPERTY, now);
            inactiveChannels.add(new ChannelFinderChannel(channel.name(), channel.owner(), properties));
        }
        store.updateChannels(inactiveChannels);
    }

    private static String resolveIocId(String sourceAddress) {
        if (sourceAddress == null || sourceAddress.isBlank()) {
            return null;
        }
        String candidate = sourceAddress.trim();
        if (candidate.startsWith("/")) {
            candidate = candidate.substring(1);
        }
        int colonIndex = candidate.lastIndexOf(':');
        if (colonIndex <= 0 || colonIndex >= candidate.length() - 1) {
            return candidate;
        }
        String hostPart = candidate.substring(0, colonIndex);
        String portPart = candidate.substring(colonIndex + 1);
        try {
            int port = Integer.parseInt(portPart);
            if (port < 0 || port > 65535) {
                return candidate;
            }
            int slashIndex = hostPart.lastIndexOf('/');
            String host = slashIndex >= 0 && slashIndex < hostPart.length() - 1
                    ? hostPart.substring(slashIndex + 1)
                    : hostPart;
            return host + ":" + port;
        } catch (NumberFormatException ignored) {
            return candidate;
        }
    }

    private static String resolveOwner(Map<String, String> clientInfos) {
        String engineer = normalized(clientInfos.get(ENGINEER_KEY));
        if (engineer != null) {
            return engineer;
        }
        String cfUsername = normalized(clientInfos.get(CF_USERNAME_KEY));
        if (cfUsername != null) {
            return cfUsername;
        }
        return DEFAULT_OWNER;
    }

    private static String resolveRecceiverId() {
        String fromHostName = normalized(System.getenv(HOSTNAME_KEY));
        if (fromHostName != null) {
            return fromHostName;
        }
        String fromComputerName = normalized(System.getenv("COMPUTERNAME"));
        if (fromComputerName != null) {
            return fromComputerName;
        }
        try {
            String localHost = normalized(InetAddress.getLocalHost().getHostName());
            if (localHost != null) {
                return localHost;
            }
        } catch (UnknownHostException ignored) {
            // Fall back to the default owner when the local host name cannot be resolved.
        }
        return DEFAULT_OWNER;
    }

    private static String normalized(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static boolean isEnabled(Map<String, String> options) {
        String fromOptions = options.get(ENABLED_KEY);
        if (fromOptions != null && !fromOptions.isBlank()) {
            return Boolean.parseBoolean(fromOptions);
        }

        String fromProperty = System.getProperty(ENABLED_KEY);
        if (fromProperty != null && !fromProperty.isBlank()) {
            return Boolean.parseBoolean(fromProperty);
        }

        String fromEnv = System.getenv("RECCEIVER_CF_ENABLED");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return Boolean.parseBoolean(fromEnv);
        }

        return false;
    }
}
