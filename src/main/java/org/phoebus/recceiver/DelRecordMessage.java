package org.phoebus.recceiver;

public class DelRecordMessage {
    private final int recId;

    public DelRecordMessage(int recId) {
        this.recId = recId;
    }

    public int getRecId() {
        return recId;
    }

    @Override
    public String toString() {
        return "DelRecordMessage{" +
                "recId=" + recId +
                '}';
    }
}

