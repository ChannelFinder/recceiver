package org.phoebus.recceiver.spi;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class LogRecSyncProcessor implements RecSyncProcessor {
    private static final Logger log = Logger.getLogger(LogRecSyncProcessor.class.getName());

    @Override
    public void commit(RecSyncTransaction transaction) {
        log.info("[LogRecSyncProcessor] commit from " + transaction.sourceAddress());
        log.info("[LogRecSyncProcessor] recordsToAdd=" + transaction.recordsToAdd().size()
                + ", recordsToDelete=" + transaction.recordsToDelete().size()
                + ", recordInfosToAdd=" + transaction.recordInfosToAdd().size()
                + ", aliases=" + transaction.aliases().size()
                + ", clientInfos=" + transaction.clientInfos().size());

        for (AddRecordMessage add : transaction.recordsToAdd().values()) {
            log.info("[LogRecSyncProcessor] ADD recId=" + add.recId()
                    + " aType=" + add.aType()
                    + " recordType='" + add.recordType() + "'"
                    + " recordName='" + add.recordName() + "'");
        }

        for (Integer recId : transaction.recordsToDelete()) {
            log.info("[LogRecSyncProcessor] DEL recId=" + recId);
        }

        for (Map.Entry<Integer, Map<String, String>> recInfoEntry : transaction.recordInfosToAdd().entrySet()) {
            log.info("[LogRecSyncProcessor] INFO recId=" + recInfoEntry.getKey()
                    + " values=" + recInfoEntry.getValue());
        }

        for (Map.Entry<Integer, List<String>> aliasEntry : transaction.aliases().entrySet()) {
            log.info("[LogRecSyncProcessor] ALIAS recId=" + aliasEntry.getKey()
                    + " aliases=" + aliasEntry.getValue());
        }

        if (!transaction.clientInfos().isEmpty()) {
            log.info("[LogRecSyncProcessor] clientInfos=" + transaction.clientInfos());
        }
    }

    @Override
    public void connectionClosed(ConnectionClosedEvent event) {
        log.info("[LogRecSyncProcessor] connectionClosed source=" + event.sourceAddress()
                + " reason='" + event.reason() + "'");
    }
}

