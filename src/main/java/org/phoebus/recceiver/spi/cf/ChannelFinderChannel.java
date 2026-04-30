package org.phoebus.recceiver.spi.cf;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

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
}
