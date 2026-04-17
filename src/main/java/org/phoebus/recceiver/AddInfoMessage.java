package org.phoebus.recceiver;

public class AddInfoMessage {
    private final int recId;
    private final int keyLength;
    private final int valueLength;
    private final String key;
    private final String value;

    public AddInfoMessage(int recId, int keyLength, int valueLength, String key, String value) {
        this.recId = recId;
        this.keyLength = keyLength;
        this.valueLength = valueLength;
        this.key = key;
        this.value = value;
    }

    public int getRecId() {
        return recId;
    }

    public int getKeyLength() {
        return keyLength;
    }

    public int getValueLength() {
        return valueLength;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "AddInfoMessage{" +
                "recId=" + recId +
                ", keyLength=" + keyLength +
                ", valueLength=" + valueLength +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}

