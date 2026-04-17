package org.phoebus.recceiver;

import org.junit.jupiter.api.Test;
import org.phoebus.recceiver.spi.AddInfoMessage;
import org.phoebus.recceiver.spi.AddRecordMessage;
import org.phoebus.recceiver.spi.DelRecordMessage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RecceiverMessageBodyParserTest {

    @Test
    void parseAddRecord() {
        byte[] recordType = "ai".getBytes(StandardCharsets.US_ASCII);
        byte[] recordName = "TEST:REC".getBytes(StandardCharsets.US_ASCII);

        ByteBuffer body = ByteBuffer.allocate(9 + recordType.length + recordName.length);
        body.putInt(42);
        body.put((byte) 0);
        body.put((byte) recordType.length);
        body.putShort((short) recordName.length);
        body.put(recordType);
        body.put(recordName);
        body.flip();

        AddRecordMessage message = RecceiverMessageBodyParser.parseAddRecord(body);

        assertEquals(42, message.recId());
        assertEquals(0, message.aType());
        assertEquals("ai", message.recordType());
        assertEquals("TEST:REC", message.recordName());
    }

    @Test
    void parseDelRecord() {
        ByteBuffer body = ByteBuffer.allocate(4);
        body.putInt(7);
        body.flip();

        DelRecordMessage message = RecceiverMessageBodyParser.parseDelRecord(body);

        assertEquals(7, message.recId());
    }

    @Test
    void parseAddInfo() {
        byte[] key = "host".getBytes(StandardCharsets.US_ASCII);
        byte[] value = "ioc-01".getBytes(StandardCharsets.US_ASCII);

        ByteBuffer body = ByteBuffer.allocate(9 + key.length + value.length);
        body.putInt(0); // client-wide info
        body.put((byte) key.length);
        body.put((byte) 0);
        body.putShort((short) value.length);
        body.put(key);
        body.put(value);
        body.flip();

        AddInfoMessage message = RecceiverMessageBodyParser.parseAddInfo(body);

        assertEquals(0, message.recId());
        assertEquals("host", message.key());
        assertEquals("ioc-01", message.value());
    }

    @Test
    void rejectBadAddRecordAlias() {
        ByteBuffer body = ByteBuffer.allocate(9);
        body.putInt(11);
        body.put((byte) 1); // alias
        body.put((byte) 2); // invalid for alias (must be zero)
        body.putShort((short) 1);
        body.put((byte) 'x');
        body.flip();

        assertThrows(IllegalArgumentException.class, () -> RecceiverMessageBodyParser.parseAddRecord(body));
    }
}
