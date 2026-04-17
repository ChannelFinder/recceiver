package org.phoebus.recceiver.spi;

public record AddRecordMessage(
        int recId,
        int aType,
        int rtLength,
        int rnLength,
        String recordType,
        String recordName
) {
}

