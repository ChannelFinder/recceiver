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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private static final String DELETE_ON_SHUTDOWN_KEY = "recceiver.cf.delete-on-shutdown";
    private static final String ALIAS_ENABLED_KEY = "recceiver.cf.alias-enabled";
    private static final String INFOTAGS_KEY = "recceiver.cf.infotags";
    private static final String ENVIRONMENT_VARS_KEY = "recceiver.cf.environment-vars";

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
    private boolean deleteOnShutdown;
    private boolean aliasEnabled;
    private Set<String> infoTagWhitelist = Set.of();
    private Map<String, String> environmentVarMappings = Map.of();
    private Set<String> managedProperties = Set.of();  // Properties managed by recceiver for property merging

    // IOC connection registry: tracks all IOCs that have connected to this processor.
    // Key = iocId (e.g., "host:port"), Value = IocInfo metadata
    // Used during shutdown to clean up channels belonging to tracked IOCs.
    private final Map<String, IocInfo> iocRegistry = new ConcurrentHashMap<>();

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
        this.deleteOnShutdown = Boolean.parseBoolean(options.getOrDefault(DELETE_ON_SHUTDOWN_KEY, "true"));
        this.aliasEnabled = Boolean.parseBoolean(options.getOrDefault(ALIAS_ENABLED_KEY, "false"));
        this.infoTagWhitelist = parseInfoTags(options.get(INFOTAGS_KEY));
        // Parse once at init so commit path only performs lookups.
        this.environmentVarMappings = parseEnvironmentVarMappings(options.get(ENVIRONMENT_VARS_KEY));
        String propertyOwner = username == null ? DEFAULT_OWNER : username;
        Set<String> requiredProperties = new LinkedHashSet<>(REQUIRED_PROPERTIES);
        if (aliasEnabled) {
            requiredProperties.add(ALIAS_PROPERTY);
        }
        requiredProperties.addAll(infoTagWhitelist);
        requiredProperties.addAll(environmentVarMappings.values());
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

        // Register this IOC in the registry for shutdown cleanup tracking.
        if (transaction.initial()) {
            String iocId = resolveIocId(transaction);
            if (iocId != null && !iocId.isBlank()) {
                Map<String, String> clientInfos = transaction.clientInfos();
                IocInfo iocInfo = new IocInfo(
                        iocId,
                        transaction.sourceAddress(),
                        transaction.sourceHost(),
                        transaction.sourcePort(),
                        normalized(clientInfos.get(IOCNAME_KEY)),
                        resolveHostName(transaction, clientInfos),
                        resolveOwner(clientInfos),
                        OffsetDateTime.now().toString()
                );
                iocRegistry.put(iocId, iocInfo);
                log.info(() -> "[CFRecSyncProcessor] registered IOC in shutdown registry: " + iocId);
            }
        }

        Collection<ChannelFinderChannel> channels = buildChannels(transaction);
        if (!channels.isEmpty()) {
            store.updateChannels(channels);
        }
    }

    private Collection<ChannelFinderChannel> buildChannels(RecSyncTransaction transaction) {
        Collection<ChannelFinderChannel> channels = new ArrayList<>();
        Map<String, String> clientInfos = transaction.clientInfos();
        Map<Integer, Map<String, String>> recordInfos = transaction.recordInfosToAdd();
        String owner = resolveOwner(clientInfos);
        // Group the commit-wide IOC/timestamp/port values once so primary and alias
        // channel builders reuse the same shared metadata.
        ChannelContext context = buildChannelContext(transaction, clientInfos);

        warnForOrphanInfoTags(transaction, recordInfos);

        addPrimaryChannels(transaction, recordInfos, owner, context, channels);

        if (aliasEnabled) {
            addAliasChannels(transaction, recordInfos, owner, context, channels);
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

    private Map<String, String> buildProperties(
            AddRecordMessage addRecord,
            Map<String, String> recordInfoProperties,
            ChannelContext context
    ) {
        Map<String, String> properties = buildStandardProperties(
                addRecord,
                context
        );
        if (recordInfoProperties != null && !recordInfoProperties.isEmpty() && !infoTagWhitelist.isEmpty()) {
            for (Map.Entry<String, String> entry : recordInfoProperties.entrySet()) {
                if (infoTagWhitelist.contains(entry.getKey())) {
                    properties.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return properties;
    }

    private static Map<String, String> buildStandardProperties(AddRecordMessage addRecord, ChannelContext context) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(HOST_NAME_PROPERTY, context.hostName());
        properties.put(IOC_NAME_PROPERTY, context.iocName());
        properties.put(PV_STATUS_PROPERTY, ACTIVE);
        properties.put(TIME_PROPERTY, context.timestamp());
        properties.put(IOC_ID_PROPERTY, context.iocId());
        properties.put(IOC_IP_PROPERTY, context.iocIp());
        properties.put(RECCEIVER_ID_PROPERTY, RECCEIVER_ID);
        properties.put(RECORD_TYPE_PROPERTY, addRecord.recordType());
        if (context.caPort() != null) {
            properties.put(CA_PORT_PROPERTY, context.caPort());
        }
        if (context.pvaPort() != null) {
            properties.put(PVA_PORT_PROPERTY, context.pvaPort());
        }
        // Apply configured ENV->property mappings resolved from clientInfos for this commit.
        properties.putAll(context.mappedEnvironmentProperties());
        return properties;
    }

    private ChannelContext buildChannelContext(RecSyncTransaction transaction, Map<String, String> clientInfos) {
        return new ChannelContext(
                resolveHostName(transaction, clientInfos),
                resolveIocName(transaction, clientInfos),
                resolveIocId(transaction),
                resolveIocIp(transaction),
                OffsetDateTime.now().toString(),
                normalized(clientInfos.get(CA_PORT_ENV_KEY)),
                normalized(clientInfos.get(PVA_PORT_ENV_KEY)),
                buildMappedEnvironmentProperties(clientInfos)
        );
    }

    private Map<String, String> buildMappedEnvironmentProperties(Map<String, String> clientInfos) {
        if (environmentVarMappings.isEmpty()) {
            return Map.of();
        }

        Map<String, String> mappedProperties = new LinkedHashMap<>();
        for (Map.Entry<String, String> envMapping : environmentVarMappings.entrySet()) {
            String value = normalized(clientInfos.get(envMapping.getKey()));
            if (value != null) {
                mappedProperties.put(envMapping.getValue(), value);
            }
        }
        return Map.copyOf(mappedProperties);
    }

    private void warnForOrphanInfoTags(RecSyncTransaction transaction, Map<Integer, Map<String, String>> recordInfos) {
        for (Map.Entry<Integer, Map<String, String>> recordInfoEntry : recordInfos.entrySet()) {
            if (!transaction.recordsToAdd().containsKey(recordInfoEntry.getKey())) {
                log.warning(() -> "[CFRecSyncProcessor] info tags for recId " + recordInfoEntry.getKey()
                        + " have no matching primary record");
            }
        }
    }

    private void addAliasChannels(
            RecSyncTransaction transaction,
            Map<Integer, Map<String, String>> recordInfos,
            String owner,
            ChannelContext context,
            Collection<ChannelFinderChannel> channels
    ) {
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
                Map<String, String> aliasProperties = buildProperties(
                        primaryRecord,
                        recordInfos.get(primaryRecord.recId()),
                        context
                );
                aliasProperties.put(ALIAS_PROPERTY, primaryRecord.recordName());
                channels.add(new ChannelFinderChannel(aliasName, owner, aliasProperties));
            }
        }
    }

    private void addPrimaryChannels(
            RecSyncTransaction transaction,
            Map<Integer, Map<String, String>> recordInfos,
            String owner,
            ChannelContext context,
            Collection<ChannelFinderChannel> channels
    ) {
        for (AddRecordMessage addRecord : transaction.recordsToAdd().values()) {
            if (addRecord.aType() != 0) {
                continue;
            }
            Map<String, String> properties = buildProperties(
                    addRecord,
                    recordInfos.get(addRecord.recId()),
                    context
            );
            channels.add(new ChannelFinderChannel(addRecord.recordName(), owner, properties));
        }
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
            iocRegistry.remove(iocId);
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
        iocRegistry.remove(iocId);
    }

    @Override
    public void destroy() {
        System.err.println("[CFRecSyncProcessor] destroy: cleaning up tracked IOCs on shutdown");
        log.info("[CFRecSyncProcessor] destroy: cleaning up tracked IOCs on shutdown");
        try {
            cleanupTrackedIocs();
        } catch (Exception e) {
            System.err.println("[CFRecSyncProcessor] error during shutdown cleanup: " + e.getMessage());
            log.severe("[CFRecSyncProcessor] error during shutdown cleanup: " + e.getMessage());
        }
    }

    /**
     * Performs cleanup of all IOCs tracked in the iocRegistry.
     * Called during application shutdown via Spring @PreDestroy lifecycle.
     * If deleteOnShutdown is true, deletes all channels belonging to tracked IOCs.
     * Otherwise, marks them as inactive.
     */
    private void cleanupTrackedIocs() {
        if (iocRegistry.isEmpty()) {
            System.err.println("[CFRecSyncProcessor] no IOCs to clean up on shutdown");
            log.info("[CFRecSyncProcessor] no IOCs to clean up on shutdown");
            return;
        }

        String msg = "[CFRecSyncProcessor] cleaning up " + iocRegistry.size() + " tracked IOCs";
        System.err.println(msg);
        log.info(msg);

        // Iterate over a copy of the registry keys to avoid ConcurrentModificationException
        Set<String> iocIds = new HashSet<>(iocRegistry.keySet());
        for (String iocId : iocIds) {
            try {
                cleanupIoc(iocId);
            } catch (Exception e) {
                String errMsg = "[CFRecSyncProcessor] error cleaning up IOC " + iocId + ": " + e.getMessage();
                System.err.println(errMsg);
                log.warning(() -> errMsg);
            }
        }
    }

    protected int iocRegistrySize() {
        return iocRegistry.size();
    }

    /**
     * Cleans up channels for a specific IOC.
     * @param iocId the IOC ID to clean up (e.g., "host:port")
     */
    private void cleanupIoc(String iocId) {
        Map<String, ChannelFinderChannel> existing = store.findByProperty(IOC_ID_PROPERTY, iocId);
        if (existing.isEmpty()) {
            String msg = "[CFRecSyncProcessor] no channels found for IOC " + iocId;
            System.err.println(msg);
            log.info(msg);
            iocRegistry.remove(iocId);
            return;
        }

        String foundMsg = "[CFRecSyncProcessor] found " + existing.size() + " channels for IOC " + iocId;
        System.err.println(foundMsg);
        log.info(foundMsg);

        if (deleteOnShutdown) {
            String deleteMsg = "[CFRecSyncProcessor] deleting " + existing.size() + " channels for IOC " + iocId;
            System.err.println(deleteMsg);
            log.info(deleteMsg);
            store.deleteChannels(existing.keySet());
        } else {
            String inactiveMsg = "[CFRecSyncProcessor] marking " + existing.size() + " channels as inactive for IOC " + iocId;
            System.err.println(inactiveMsg);
            log.info(inactiveMsg);
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

        iocRegistry.remove(iocId);
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

    private static Set<String> parseInfoTags(String configuredInfoTags) {
        String normalizedInfoTags = normalized(configuredInfoTags);
        if (normalizedInfoTags == null) {
            return Set.of();
        }

        Set<String> parsed = new LinkedHashSet<>();
        for (String token : normalizedInfoTags.split("[,\\s]+")) {
            String infoTag = normalized(token);
            if (infoTag != null) {
                parsed.add(infoTag);
            }
        }
        return Set.copyOf(parsed);
    }

    /**
     * Parses mappings in the format "ENV_VAR:PropertyName,OTHER_ENV:OtherProperty".
     * @param configuredMappings the raw input string containing environment variable to property name mappings
     * @return a map of environment variable name to channel property name for each valid mapping parsed from the input string
     */
    private static Map<String, String> parseEnvironmentVarMappings(String configuredMappings) {
        String normalizedMappings = normalized(configuredMappings);
        if (normalizedMappings == null) {
            return Map.of();
        }

        Map<String, String> parsed = new LinkedHashMap<>();
        for (String token : normalizedMappings.split(",")) {
            String mapping = normalized(token);
            if (mapping == null) {
                continue;
            }
            int separatorIndex = mapping.indexOf(':');
            if (separatorIndex <= 0 || separatorIndex >= mapping.length() - 1) {
                throw new IllegalArgumentException(
                        "Invalid environment variable mapping '" + mapping + "'. Expected format ENV:PropertyName");
            }
            String envVar = normalized(mapping.substring(0, separatorIndex));
            String propertyName = normalized(mapping.substring(separatorIndex + 1));
            if (envVar == null || propertyName == null) {
                throw new IllegalArgumentException(
                        "Invalid environment variable mapping '" + mapping + "'. Expected format ENV:PropertyName");
            }
            // Keep the last mapping if a variable appears multiple times.
            parsed.put(envVar, propertyName);
        }
        return Map.copyOf(parsed);
    }

    // Shared metadata copied into every channel emitted for a single commit.
    private record ChannelContext(
            String hostName,
            String iocName,
            String iocId,
            String iocIp,
            String timestamp,
            String caPort,
            String pvaPort,
            Map<String, String> mappedEnvironmentProperties
    ) {
    }

    // Metadata about a connected IOC, tracked for shutdown cleanup.
    private record IocInfo(
            String iocId,
            String sourceAddress,
            String sourceHost,
            Integer sourcePort,
            String iocName,
            String hostName,
            String owner,
            String connectedAt
    ) {
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
