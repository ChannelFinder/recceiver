package org.phoebus.recceiver.spi;

import java.util.logging.Logger;

public class LogRecSyncProcessor implements RecSyncProcessor {
    private static final Logger log = Logger.getLogger(LogRecSyncProcessor.class.getName());

    @Override
    public void commit(RecSyncTransaction transaction) {
        log.info("[LogRecSyncProcessor] commit from " + transaction.sourceAddress());
        log.info("[LogRecSyncProcessor] recordsToAdd=" + transaction.recordsToAdd().size()
                + ", recordsToDelete=" + transaction.recordsToDelete().size()
                + ", infoMessages=" + transaction.infoMessages().size()
                + ", clientInfos=" + transaction.clientInfos().size());

        for (AddRecordMessage add : transaction.recordsToAdd()) {
            log.info("[LogRecSyncProcessor] ADD recId=" + add.recId()
                    + " aType=" + add.aType()
                    + " recordType='" + add.recordType() + "'"
                    + " recordName='" + add.recordName() + "'");
        }

        for (DelRecordMessage del : transaction.recordsToDelete()) {
            log.info("[LogRecSyncProcessor] DEL recId=" + del.recId());
        }

        for (AddInfoMessage info : transaction.infoMessages()) {
            log.info("[LogRecSyncProcessor] INFO recId=" + info.recId()
                    + " key='" + info.key() + "'"
                    + " value='" + info.value() + "'");
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

