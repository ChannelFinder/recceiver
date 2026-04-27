package org.phoebus.recceiver.spi.cf;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface ChannelFinderStore {
    Map<String, ChannelFinderChannel> findByNames(Set<String> channelNames);

    Map<String, ChannelFinderChannel> findByProperty(String propertyName, String propertyValue);

    void updateChannels(Collection<ChannelFinderChannel> channels);

    void deleteChannels(Collection<String> channelNames);

    default void ensureProperties(Set<String> propertyNames, String owner) {
        // Optional capability for stores backed by real ChannelFinder services.
    }
}
