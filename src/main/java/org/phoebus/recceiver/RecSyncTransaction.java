package org.phoebus.recceiver;

import java.util.List;
import java.util.Map;

public record RecSyncTransaction(
        String sourceAddress,
        List<AddRecordMessage> recordsToAdd,
        List<DelRecordMessage> recordsToDelete,
        List<AddInfoMessage> infoMessages,
        Map<String, String> clientInfos
) {
    public RecSyncTransaction {
        recordsToAdd = List.copyOf(recordsToAdd);
        recordsToDelete = List.copyOf(recordsToDelete);
        infoMessages = List.copyOf(infoMessages);
        clientInfos = Map.copyOf(clientInfos);
    }
}

