package org.phoebus.recceiver.spi;

public record AddInfoMessage(
        int recId,
        int keyLength,
        int valueLength,
        String key,
        String value
) {
}

