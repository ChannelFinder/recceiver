package org.phoebus.recceiver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.phoebus.recceiver.spi.AddInfoMessage;
import org.phoebus.recceiver.spi.AddRecordMessage;
import org.phoebus.recceiver.spi.ConnectionClosedEvent;
import org.phoebus.recceiver.spi.DelRecordMessage;
import org.phoebus.recceiver.spi.RecSyncProcessorDispatcher;
import org.phoebus.recceiver.spi.RecSyncTransaction;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MessageProcessor {
    private static final Logger log = Logger.getLogger(MessageProcessor.class.getName());

    private final SocketChannel channel;
    private final String connectionSourceAddress;
    private final long pingIntervalSeconds;
    private final int serverKey;
    private final RecSyncProcessorDispatcher processorDispatcher;
    private final Random pingRandom = new Random();
    /** Size in bytes of the read buffer allocated on each call to {@link #process()}. */
    private final int readBufferSize;

    private boolean processingPartialMessage = false;
    private volatile Integer outstandingPingNonce;
    private volatile boolean pingLoopStarted = false;
    private volatile boolean connectionClosedNotified = false;
    private boolean initialCommitPending = true;
    private Thread pingThread;

    private final List<AddRecordMessage> recordsToAdd = new ArrayList<>();
    private final List<DelRecordMessage> recordsToDelete = new ArrayList<>();
    private final List<AddInfoMessage> infoMessages = new ArrayList<>();
    private final Map<String, String> clientInfos = new LinkedHashMap<>();

    ConcurrentLinkedQueue<Message> completeMessages = new ConcurrentLinkedQueue<>();
    Message partialMessage;
    ByteBuffer leftOver;

    public MessageProcessor(SocketChannel channel, long pingIntervalSeconds, int serverKey, RecSyncProcessorDispatcher processorDispatcher, int readBufferSize) {
        this.channel = channel;
        this.connectionSourceAddress = resolveSourceAddress(channel);
        this.pingIntervalSeconds = pingIntervalSeconds;
        this.serverKey = serverKey;
        this.processorDispatcher = processorDispatcher == null ? RecSyncProcessorDispatcher.empty() : processorDispatcher;
        this.readBufferSize = Math.max(8, readBufferSize); // header is 8 bytes; enforce minimum
    }

    public void process() throws IOException {
        ByteBuffer readBuffer = ByteBuffer.allocate(readBufferSize);

        if (leftOver != null) {
            log.finer("copying leftover bytes=" + leftOver.remaining());
            leftOver.flip();
            readBuffer.put(leftOver);
            leftOver = null;
        }
        int bytesRead = channel.read(readBuffer);
        if (log.isLoggable(Level.FINER)) {
            log.finer("read bytes=" + bytesRead + " from " + sourceAddress());
        }
        readBuffer.flip();
        if (bytesRead == -1) {
            log.info("EOF detected - closing connection for " + sourceAddress());
            shutdown("eof");
            channel.close();
            return;
        }
        if (bytesRead == 0) {
            log.finer("No data available");
            return;
        }

        while (readBuffer.hasRemaining()) {
            if (log.isLoggable(Level.FINER)) {
                log.finer("remaining buffer pos=" + readBuffer.position()
                        + " limit=" + readBuffer.limit()
                        + " remaining=" + readBuffer.remaining()
                        + " partial=" + processingPartialMessage);
            }
            // a brand new message
            if (!processingPartialMessage && readBuffer.remaining() >= 8) {
                processingPartialMessage = true;
                Message message = processHeader(readBuffer);
                if (message == null) {
                    processingPartialMessage = false;
                    partialMessage = null;
                    break;
                }
                log.finer("Processing new message; body bytes available=" + readBuffer.remaining());
                if (readBuffer.remaining() >= message.msgLen) {
                    copyBytes(readBuffer, message.getMsgBody(), message.msgLen);
                    message.getMsgBody().flip();
                    // now we have a complete message put it in the queue
                    completeMessages.add(message);
                    processingPartialMessage = false;
                } else {
                    log.finer("Partial message: available=" + readBuffer.remaining() + " need=" + message.msgLen);
                    if (readBuffer.remaining() > 0) {
                        try {
                            log.finer("before transfer bodyRemaining=" + message.msgBody.remaining() + " readRemaining=" + readBuffer.remaining());
                            message.getMsgBody().put(readBuffer);
                            log.finer("after transfer bodyRemaining=" + message.msgBody.remaining() + " readRemaining=" + readBuffer.remaining());
                        } catch (Exception e) {
                            log.log(Level.WARNING, "Failed while buffering partial message", e);
                        }
                        log.finer("Partial message buffered; still expecting=" + message.getRemainingBytes());
                    }
                    partialMessage = message;
                }
            } else if (processingPartialMessage && readBuffer.hasRemaining()) {
                log.finer("Processing partial message available=" + readBuffer.remaining() + " need=" + partialMessage.getRemainingBytes());
                if (readBuffer.remaining() >= partialMessage.getRemainingBytes()) {
                    int remainingBytes = partialMessage.getRemainingBytes();
                    copyBytes(readBuffer, partialMessage.getMsgBody(), remainingBytes);
                    partialMessage.getMsgBody().flip();
                    completeMessages.add(partialMessage);
                    partialMessage = null;
                    processingPartialMessage = false;
                } else {
                    partialMessage.getMsgBody().put(readBuffer);
                }
            } else {
                log.finer("Incomplete header fragment buffered; remaining=" + readBuffer.remaining());
                leftOver = ByteBuffer.allocate(readBuffer.remaining());
                leftOver.put(readBuffer);
                break;
            }

            log.finer("complete message count=" + completeMessages.size());
            // check for complete messages
            completeMessages.stream().forEach(message -> {
                log.finer("processing message id=" + String.format("%04x", (int) message.msgId) + " len=" + message.msgLen);
                switch (message.msgId) {
                    case 0x0001:
                        log.finer("received 0x0001 bodyRemaining=" + message.msgBody.remaining());
                        message.msgBody.getChar();
                        message.msgBody.getChar();
                        int returnedKey = message.msgBody.getInt();

                        // validate
                        if (returnedKey == serverKey) {
                            // send the reply
                            ByteBuffer serverReply = ByteBuffer.allocate(9);
                            serverReply.put("RC".getBytes(StandardCharsets.US_ASCII));
                            serverReply.putChar(RecceiverApplication.SERVER_GREET_MSG_ID);
                            serverReply.putInt(1);
                            serverReply.put((byte) 0x00);
                            serverReply.flip();
                            try {
                                channel.write(serverReply);
                            } catch (IOException e) {
                                log.log(Level.WARNING, "Failed to send server greet", e);
                            }
                            serverReply.clear();
                        }
                        break;
                    case 0x0002:
                        log.finer("received 0x0002 (pong)");
                        if (message.msgBody.remaining() >= Integer.BYTES) {
                            int nonce = message.msgBody.getInt();
                            if (outstandingPingNonce != null && outstandingPingNonce == nonce) {
                                log.finer("received matching pong for nonce " + nonce);
                                outstandingPingNonce = null;
                            } else {
                                log.fine("received unexpected pong nonce " + nonce);
                            }
                        }
                        break;
                    case 0x0003:
                        log.finer("received 0x0003 (add record)");
                        try {
                            AddRecordMessage addRecordMessage = RecceiverMessageBodyParser.parseAddRecord(message.msgBody);
                            recordsToAdd.add(addRecordMessage);
                            log.fine("Add record: " + addRecordMessage);
                        } catch (Exception e) {
                            log.log(Level.WARNING, "Failed to parse add record message", e);
                        }
                        break;
                    case 0x0004:
                        try {
                            DelRecordMessage delRecordMessage = RecceiverMessageBodyParser.parseDelRecord(message.msgBody);
                            recordsToDelete.add(delRecordMessage);
                            log.fine("Delete record: " + delRecordMessage);
                        } catch (Exception e) {
                            log.log(Level.WARNING, "Failed to parse delete message", e);
                        }
                        break;
                    case 0x0005:
                        // The IOC signals that its record upload is complete.
                        // Dispatch the transaction asynchronously (CF upload runs on a virtual thread)
                        // so this message-reading thread is immediately free for ping/pong.
                        // Keeping ping/pong running during CF upload prevents the IOC from closing
                        // the connection due to missed pings on large initial batches.
                        log.info("received 0x0005 (IOC upload complete) source=" + sourceAddress());
                        if (log.isLoggable(Level.FINER)) {
                            log.finer("upload done payload=" + String.valueOf(StandardCharsets.US_ASCII.decode(message.msgBody)));
                        }
                        dispatchTransaction();
                        startPingLoop();
                        break;
                    case 0x0006:
                        log.finer("received 0x0006 (add info) bytes=" + message.msgBody.remaining());
                        try {
                            AddInfoMessage addInfoMessage = RecceiverMessageBodyParser.parseAddInfo(message.msgBody);
                            infoMessages.add(addInfoMessage);
                            if (addInfoMessage.recId() == 0) {
                                clientInfos.put(addInfoMessage.key(), addInfoMessage.value());
                            }
                            log.fine("Add info: " + addInfoMessage);
                        } catch (Exception e) {
                            log.log(Level.WARNING, "Failed to parse info message", e);
                        }
                        break;
                }
            });
            completeMessages.clear();
        }
    }

    private Message processHeader(ByteBuffer readBuffer) {
        // Decode the header
        byte[] idArray = new byte[2];
        readBuffer.get(idArray, 0, 2);

        String id = String.valueOf(StandardCharsets.US_ASCII.decode(ByteBuffer.wrap(idArray)).array());
        char msgId = readBuffer.getChar();
        int msgLen = readBuffer.getInt();
        log.finer("processing header id=" + id + " msgId=" + String.format("%04x", (int) msgId) + " len=" + msgLen);
        if (!"RC".equals(id) || msgLen < 0) {
            // invalid id... ignore
            log.fine("Ignoring invalid header id=" + id + " len=" + msgLen);
            return null;
        }
        return new Message(id, msgId, msgLen);
    }

    private synchronized void startPingLoop() {
        if (pingLoopStarted) {
            return;
        }

        pingLoopStarted = true;
        pingThread = Thread.ofVirtual().name("recceiver-ping-", 0).start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    TimeUnit.SECONDS.sleep(pingIntervalSeconds);
                    sendPing();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (IOException e) {
                    shutdown("ping loop io error");
                    try {
                        channel.close();
                    } catch (IOException ex) {
                        log.log(Level.WARNING, "Failed to close channel after ping loop error", ex);
                    }
                    break;
                }
            }
        });
    }

    private synchronized void sendPing() throws IOException {
        if (!channel.isOpen()) {
            shutdown();
            return;
        }

        if (outstandingPingNonce != null) {
            log.info("closing channel due to missed pong for nonce " + outstandingPingNonce);
            shutdown("missed pong");
            channel.close();
            return;
        }

        int nonce = pingRandom.nextInt();
        ByteBuffer pingMessage = ByteBuffer.allocate(12);
        pingMessage.put("RC".getBytes(StandardCharsets.US_ASCII));
        pingMessage.putChar(RecceiverApplication.PING_MSG_ID);
        pingMessage.putInt(Integer.BYTES);
        pingMessage.putInt(nonce);
        pingMessage.flip();

        channel.write(pingMessage);
        outstandingPingNonce = nonce;
        log.fine("sent ping with nonce " + nonce);
    }

    /**
     * Copy exactly {@code length} bytes from the source buffer's current position into the
     * destination buffer, then advance the source position by the same amount.
     *
     * We use a slice here so we copy from the source buffer's current position rather than
     * from index 0 of the backing array. That matters because the message header has already
     * been consumed before body bytes are copied, and TCP reads may also contain partial or
     * back-to-back messages in the same buffer.
     *
     * After the copy:
     * - the destination buffer position is advanced by {@code length}
     * - the source buffer position is also advanced by {@code length}
     * - the source buffer limit is unchanged
     */
    private void copyBytes(ByteBuffer source, ByteBuffer destination, int length) {
        ByteBuffer slice = source.slice();
        slice.limit(length);
        destination.put(slice);
        source.position(source.position() + length);
    }

    public synchronized void shutdown() {
        shutdown("shutdown");
    }

    public synchronized void shutdown(String reason) {
        if (pingThread != null) {
            pingThread.interrupt();
            pingThread = null;
        }
        pingLoopStarted = false;
        outstandingPingNonce = null;
        notifyConnectionClosed(reason);
    }

    private void dispatchTransaction() {
        RecSyncTransaction transaction = new RecSyncTransaction(
                sourceAddress(),
                List.copyOf(recordsToAdd),
                List.copyOf(recordsToDelete),
                List.copyOf(infoMessages),
                Map.copyOf(clientInfos),
                initialCommitPending,
                true);
        // Clear local state immediately so incoming messages can accumulate for the next transaction
        // regardless of how long the current dispatch takes.
        initialCommitPending = false;
        recordsToAdd.clear();
        recordsToDelete.clear();
        infoMessages.clear();
        clientInfos.clear();

        // Always dispatch asynchronously. The message-reading thread must remain free to process
        // pong replies during CF uploads. For large initial batches (100k+ channels) the upload
        // can take minutes; blocking this thread would prevent any pings from being sent and the
        // IOC would close the connection with an EOF before the upload finishes.
        Thread.ofVirtual().name("recceiver-commit-", 0).start(() ->
                processorDispatcher.dispatchCommit(transaction));
    }

    private void notifyConnectionClosed(String reason) {
        if (connectionClosedNotified) {
            return;
        }
        connectionClosedNotified = true;
        processorDispatcher.dispatchConnectionClosed(new ConnectionClosedEvent(sourceAddress(), reason));
    }

    private String sourceAddress() {
        return connectionSourceAddress;
    }

    private static String resolveSourceAddress(SocketChannel channel) {
        try {
            return String.valueOf(channel.getRemoteAddress());
        } catch (IOException e) {
            return "unknown";
        }
    }
}

