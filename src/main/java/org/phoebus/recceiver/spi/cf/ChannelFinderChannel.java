package org.phoebus.recceiver.spi.cf;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public record ChannelFinderChannel(String name, String owner, Map<String, String> properties) {
    public ChannelFinderChannel {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(properties, "properties");
        properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    public String property(String propertyName) {
        return properties.get(propertyName);
    }

    public ChannelFinderChannel mergeUnmanagedProperties(ChannelFinderChannel existing, Set<String> managedPropertyNames) {
        if (existing == null) {
            return this;
        }

        Map<String, String> merged = new LinkedHashMap<>(existing.properties());
        merged.putAll(properties);
        for (String managedPropertyName : managedPropertyNames) {
            if (!properties.containsKey(managedPropertyName)) {
                merged.remove(managedPropertyName);
            }
        }
        return new ChannelFinderChannel(name, owner, merged);
    }
}
