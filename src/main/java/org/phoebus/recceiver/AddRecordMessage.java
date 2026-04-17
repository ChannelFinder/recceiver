package org.phoebus.recceiver;

public class AddRecordMessage {
    private final int recId;
    private final int aType;
    private final int rtLength;
    private final int rnLength;
    private final String recordType;
    private final String recordName;

    public AddRecordMessage(int recId, int aType, int rtLength, int rnLength, String recordType, String recordName) {
        this.recId = recId;
        this.aType = aType;
        this.rtLength = rtLength;
        this.rnLength = rnLength;
        this.recordType = recordType;
        this.recordName = recordName;
    }

    public int getRecId() {
        return recId;
    }

    public int getAType() {
        return aType;
    }

    public int getRtLength() {
        return rtLength;
    }

    public int getRnLength() {
        return rnLength;
    }

    public String getRecordType() {
        return recordType;
    }

    public String getRecordName() {
        return recordName;
    }

    @Override
    public String toString() {
        return "AddRecordMessage{" +
                "recId=" + recId +
                ", aType=" + aType +
                ", rtLength=" + rtLength +
                ", rnLength=" + rnLength +
                ", recordType='" + recordType + '\'' +
                ", recordName='" + recordName + '\'' +
                '}';
    }
}

