package org.phoebus.recceiver.spi.cf;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class InMemoryChannelFinderStore implements ChannelFinderStore {
    private final Map<String, ChannelFinderChannel> channelStore = new LinkedHashMap<>();

    @Override
    public synchronized Map<String, ChannelFinderChannel> findByNames(Set<String> channelNames) {
        Map<String, ChannelFinderChannel> found = new LinkedHashMap<>();
        for (String channelName : channelNames) {
            ChannelFinderChannel channel = channelStore.get(channelName);
            if (channel != null) {
                found.put(channelName, channel);
            }
        }
        return Map.copyOf(found);
    }

    @Override
    public synchronized Map<String, ChannelFinderChannel> findByProperty(String propertyName, String propertyValue) {
        Map<String, ChannelFinderChannel> found = new LinkedHashMap<>();
        for (Map.Entry<String, ChannelFinderChannel> entry : channelStore.entrySet()) {
            String value = entry.getValue().property(propertyName);
            if (propertyValue.equals(value)) {
                found.put(entry.getKey(), entry.getValue());
            }
        }
        return Map.copyOf(found);
    }

    @Override
    public synchronized void updateChannels(Collection<ChannelFinderChannel> channels) {
        for (ChannelFinderChannel channel : channels) {
            channelStore.put(channel.name(), channel);
        }
    }

    @Override
    public synchronized void deleteChannels(Collection<String> channelNames) {
        for (String channelName : channelNames) {
            channelStore.remove(channelName);
        }
    }

    public synchronized void put(ChannelFinderChannel channel) {
        channelStore.put(channel.name(), channel);
    }

    public synchronized Map<String, ChannelFinderChannel> channelsView() {
        return Map.copyOf(channelStore);
    }
}
