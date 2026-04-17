package org.phoebus.recceiver;

public record AddInfoMessage(
        int recId,
        int keyLength,
        int valueLength,
        String key,
        String value
) {
}
