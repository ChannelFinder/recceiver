package org.phoebus.recceiver.spi.cf;

import org.phoebus.recceiver.spi.AddRecordMessage;
import org.phoebus.recceiver.spi.ConnectionClosedEvent;
import org.phoebus.recceiver.spi.RecSyncProcessor;
import org.phoebus.recceiver.spi.RecSyncTransaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * Publishes primary record adds to ChannelFinder.
 */
public class CFRecSyncProcessor implements RecSyncProcessor {
    private static final Logger log = Logger.getLogger(CFRecSyncProcessor.class.getName());
    private static final String ENABLED_KEY = "recceiver.cf.enabled";
    private static final String SERVICE_URL_KEY = "recceiver.cf.service-url";

    private final ChannelFinderStore store;

    public CFRecSyncProcessor(ChannelFinderStore store) {
        this.store = store;
    }

    @Override
    public RecSyncProcessor build(String name, Map<String, String> options) {
        if (!isEnabled(options)) {
            return null;
        }
        String serviceUrl = options.get(SERVICE_URL_KEY);
        if (serviceUrl == null || serviceUrl.isBlank()) {
            throw new IllegalArgumentException("recceiver.cf.service-url must be configured");
        }
        ChannelFinderStore store = new ChannelFinderHttpStore(serviceUrl);
        return new CFRecSyncProcessor(store);
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
        String owner = transaction.sourceHost() != null ? transaction.sourceHost() : transaction.sourceAddress();

        for (AddRecordMessage record : transaction.recordsToAdd().values()) {
            // Primary record only (aType=0), skip aliases (aType=1)
            if (record.aType() != 0) {
                continue;
            }

            Map<String, String> properties = new LinkedHashMap<>();
            properties.put(ChannelFinderCache.IOC_ID_PROPERTY, transaction.sourceAddress());

            ChannelFinderChannel channel = new ChannelFinderChannel(record.recordName(), owner, properties);
            channels.add(channel);
        }

        return channels;
    }

    @Override
    public void connectionClosed(ConnectionClosedEvent event) {
        log.info(() -> "[CFRecSyncProcessor] connectionClosed source=" + event.sourceAddress()
                + " reason='" + event.reason() + "'");
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
