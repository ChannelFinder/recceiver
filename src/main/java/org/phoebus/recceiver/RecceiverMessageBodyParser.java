package org.phoebus.recceiver;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.phoebus.recceiver.spi.AddInfoMessage;
import org.phoebus.recceiver.spi.AddRecordMessage;
import org.phoebus.recceiver.spi.DelRecordMessage;

public final class RecceiverMessageBodyParser {
    private static final int ADD_RECORD_MIN_LENGTH = 9;
    private static final int DEL_RECORD_MIN_LENGTH = Integer.BYTES;
    private static final int ADD_INFO_MIN_LENGTH = 9;

    private RecceiverMessageBodyParser() {
    }

    public static AddRecordMessage parseAddRecord(ByteBuffer body) {
        ByteBuffer buffer = body.asReadOnlyBuffer();
        if (buffer.remaining() < ADD_RECORD_MIN_LENGTH) {
            throw new IllegalArgumentException("Add Record body too short: " + buffer.remaining());
        }

        int recId = buffer.getInt();
        int aType = Byte.toUnsignedInt(buffer.get());
        int rtLength = Byte.toUnsignedInt(buffer.get());
        int rnLength = Short.toUnsignedInt(buffer.getShort());

        if (recId <= 0) {
            throw new IllegalArgumentException("Add Record RECID must be > 0");
        }
        if (aType != 0 && aType != 1) {
            throw new IllegalArgumentException("Add Record ATYPE must be 0 or 1");
        }
        if (rnLength <= 0) {
            throw new IllegalArgumentException("Add Record RNLEN must be > 0");
        }
        if (aType == 1 && rtLength != 0) {
            throw new IllegalArgumentException("Add Record alias (ATYPE=1) requires RTLEN=0");
        }

        int variableLength = rtLength + rnLength;
        if (buffer.remaining() < variableLength) {
            throw new IllegalArgumentException("Add Record variable fields exceed body length");
        }

        byte[] recordTypeBytes = new byte[rtLength];
        byte[] recordNameBytes = new byte[rnLength];
        buffer.get(recordTypeBytes);
        buffer.get(recordNameBytes);

        String recordType = new String(recordTypeBytes, StandardCharsets.US_ASCII);
        String recordName = new String(recordNameBytes, StandardCharsets.US_ASCII);
        return new AddRecordMessage(recId, aType, rtLength, rnLength, recordType, recordName);
    }

    public static DelRecordMessage parseDelRecord(ByteBuffer body) {
        ByteBuffer buffer = body.asReadOnlyBuffer();
        if (buffer.remaining() < DEL_RECORD_MIN_LENGTH) {
            throw new IllegalArgumentException("Del Record body too short: " + buffer.remaining());
        }

        int recId = buffer.getInt();
        if (recId <= 0) {
            throw new IllegalArgumentException("Del Record RECID must be > 0");
        }
        return new DelRecordMessage(recId);
    }

    public static AddInfoMessage parseAddInfo(ByteBuffer body) {
        ByteBuffer buffer = body.asReadOnlyBuffer();
        if (buffer.remaining() < ADD_INFO_MIN_LENGTH) {
            throw new IllegalArgumentException("Add Info body too short: " + buffer.remaining());
        }

        int recId = buffer.getInt();
        int keyLength = Byte.toUnsignedInt(buffer.get());
        buffer.get(); // reserved byte
        int valueLength = Short.toUnsignedInt(buffer.getShort());

        if (keyLength <= 0) {
            throw new IllegalArgumentException("Add Info KEYLEN must be > 0");
        }

        int variableLength = keyLength + valueLength;
        if (buffer.remaining() < variableLength) {
            throw new IllegalArgumentException("Add Info variable fields exceed body length");
        }

        byte[] keyBytes = new byte[keyLength];
        byte[] valueBytes = new byte[valueLength];
        buffer.get(keyBytes);
        buffer.get(valueBytes);

        String key = new String(keyBytes, StandardCharsets.US_ASCII);
        String value = new String(valueBytes, StandardCharsets.US_ASCII);
        return new AddInfoMessage(recId, keyLength, valueLength, key, value);
    }
}
