package org.phoebus.recceiver;

import java.nio.ByteBuffer;

public class Message {
        String id;
        char msgId;
        int msgLen;

        ByteBuffer msgBody;

        public Message(String id, char msgId, int msgLen) {
            this.id = id;
            this.msgId = msgId;
            this.msgLen = msgLen;
            this.msgBody = ByteBuffer.allocate(msgLen);
        }

        public String getId() {
            return id;
        }

        public char getMsgId() {
            return msgId;
        }

        public int getMsgLen() {
            return msgLen;
        }

        public ByteBuffer getMsgBody() {
            return msgBody;
        }

        public int getRemainingBytes() {
            return msgLen - msgBody.position();
        }

    }