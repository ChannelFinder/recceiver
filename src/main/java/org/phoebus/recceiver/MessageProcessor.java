package org.phoebus.recceiver;

import javax.xml.bind.DatatypeConverter;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MessageProcessor {
    private final SocketChannel channel;
    private final long pingIntervalSeconds;
    private final int serverKey;
    private final RecSyncProcessorDispatcher processorDispatcher;
    private final Random pingRandom = new Random();

    private boolean processingPartialMessage = false;
    private volatile Integer outstandingPingNonce;
    private volatile boolean pingLoopStarted = false;
    private volatile boolean connectionClosedNotified = false;
    private ScheduledExecutorService pingService;

    private final List<AddRecordMessage> recordsToAdd = new ArrayList<>();
    private final List<DelRecordMessage> recordsToDelete = new ArrayList<>();
    private final List<AddInfoMessage> infoMessages = new ArrayList<>();
    private final Map<String, String> clientInfos = new LinkedHashMap<>();

    ConcurrentLinkedQueue<Message> completeMessages = new ConcurrentLinkedQueue<>();
    Message partialMessage;
    ByteBuffer leftOver;

    public MessageProcessor(SocketChannel channel, long pingIntervalSeconds, int serverKey, RecSyncProcessorDispatcher processorDispatcher) {
        this.channel = channel;
        this.pingIntervalSeconds = pingIntervalSeconds;
        this.serverKey = serverKey;
        this.processorDispatcher = processorDispatcher == null ? RecSyncProcessorDispatcher.empty() : processorDispatcher;
    }

    public void process() throws IOException {
        System.out.println("..................................");
        ByteBuffer readBuffer = ByteBuffer.allocate(10);

        if (leftOver != null) {
            System.out.println("copying leftover : " + leftOver.remaining() + " " + DatatypeConverter.printHexBinary(leftOver.array()));
            leftOver.flip();
            readBuffer.put(leftOver);
            leftOver = null;
        }
        int bytesRead = channel.read(readBuffer);
        System.out.println(DatatypeConverter.printHexBinary(readBuffer.array()));
        readBuffer.flip();
        System.out.println(DatatypeConverter.printHexBinary(readBuffer.array()) + " " + readBuffer.remaining());
        if (bytesRead == -1) {
            System.out.println("EOF detected - closing connection");
            shutdown("eof");
            channel.close();
            return;
        }
        if (bytesRead == 0) {
            System.out.println("No data available");
            return;
        }

        while (readBuffer.hasRemaining()) {
            System.out.println("remaining buffer is " + readBuffer.position() + " " + readBuffer.limit() + " " + readBuffer.remaining() + " " + processingPartialMessage + " " + partialMessage);
            // a brand new message
            if (!processingPartialMessage && readBuffer.remaining() >= 8) {
                processingPartialMessage = true;
                Message message = processHeader(readBuffer);
                if (message == null) {
                    processingPartialMessage = false;
                    partialMessage = null;
                    break;
                }
                System.out.println("Processing a new message... Buffer size pre processing " + readBuffer.remaining());
                if (readBuffer.remaining() >= message.msgLen) {
                    copyBytes(readBuffer, message.getMsgBody(), message.msgLen);
                    message.getMsgBody().flip();
                    // now we have a complete message put it in the queue
                    completeMessages.add(message);
                    processingPartialMessage = false;
                } else {
                    System.out.println("PARTIAL message: " + readBuffer.remaining() + " available but need " + message.msgLen);
                    if (readBuffer.remaining() > 0) {
                        try {
                            System.out.println("before transfer : " + message.msgBody.remaining() + " : readbuffer " + readBuffer.remaining());
                            message.getMsgBody().put(readBuffer);
                            System.out.println("after transfer : " + message.msgBody.remaining() + " : readbuffer " + readBuffer.remaining());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        System.out.println("PARTIAL message is moved still expecting " + message.getRemainingBytes());
                    }
                    partialMessage = message;
                }
            } else if (processingPartialMessage && readBuffer.hasRemaining()) {
                System.out.println("Processing partial Message " + readBuffer.remaining() + " " + partialMessage.getRemainingBytes());
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
                System.out.println("Processing a new incomplete message... Buffer size pre processing " + readBuffer.position() + " " + readBuffer.remaining());
                System.out.println(DatatypeConverter.printHexBinary(readBuffer.array()));
                leftOver = ByteBuffer.allocate(readBuffer.remaining());
                leftOver.put(readBuffer);
                System.out.println("Processing a new incomplete message... Buffer size processing " + readBuffer.position() + " " + readBuffer.remaining());
                System.out.println(DatatypeConverter.printHexBinary(readBuffer.array()));
                break;
            }

            System.out.println("complete message count: " + completeMessages.size());
            // check for complete messages
            completeMessages.stream().forEach(message -> {
                System.out.println("processing completed message " + message.id + " " + String.format("%04x", (int) message.msgId) + " " + message.msgLen);
                switch (message.msgId) {
                    case 0x0001:
                        System.out.println("received 0001 " + message.msgBody.remaining());
                        char clientGreet = message.msgBody.getChar();
                        char clientGreetPadding = message.msgBody.getChar();
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
                                e.printStackTrace();
                            }
                            serverReply.clear();
                        }
                        break;
                    case 0x0002:
                        System.out.println("received 0002");
                        if (message.msgBody.remaining() >= Integer.BYTES) {
                            int nonce = message.msgBody.getInt();
                            if (outstandingPingNonce != null && outstandingPingNonce == nonce) {
                                System.out.println("received matching pong for nonce " + nonce);
                                outstandingPingNonce = null;
                            } else {
                                System.out.println("received unexpected pong nonce " + nonce);
                            }
                        }
                        break;
                    case 0x0003:
                        System.out.println("received 0003");
                        try {
                            AddRecordMessage addRecordMessage = RecceiverMessageBodyParser.parseAddRecord(message.msgBody);
                            recordsToAdd.add(addRecordMessage);
                            System.out.println("Add record : " + addRecordMessage);
                        } catch (Exception e) {
                            System.out.println("failed to read " + e.getMessage());
                        }
                        break;
                    case 0x0004:
                        try {
                            DelRecordMessage delRecordMessage = RecceiverMessageBodyParser.parseDelRecord(message.msgBody);
                            recordsToDelete.add(delRecordMessage);
                            System.out.println("received 0004 - DELETE " + delRecordMessage);
                        } catch (Exception e) {
                            System.out.println("failed to read delete message " + e.getMessage());
                        }
                        break;
                    case 0x0005:
                        System.out.println("received 0005 - upload completed... start ping pong");
                        System.out.println(String.valueOf(StandardCharsets.US_ASCII.decode(message.msgBody)));
                        dispatchTransaction();
                        startPingLoop();
                        break;
                    case 0x0006:
                        System.out.println("received 0006 - Add info " + message.msgBody.remaining());
                        try {
                            AddInfoMessage addInfoMessage = RecceiverMessageBodyParser.parseAddInfo(message.msgBody);
                            infoMessages.add(addInfoMessage);
                            if (addInfoMessage.recId() == 0) {
                                clientInfos.put(addInfoMessage.key(), addInfoMessage.value());
                            }
                            System.out.println("Add info : " + addInfoMessage);
                        } catch (Exception e) {
                            System.out.println("failed to read info message " + e.getMessage());
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
        System.out.println("processing message header for " + id + " " + String.format("%04x", (int) msgId) + " " + msgLen);
        if (!"RC".equals(id) || msgLen < 0) {
            // invalid id... ignore
            return null;
        }
        return new Message(id, msgId, msgLen);
    }

    private synchronized void startPingLoop() {
        if (pingLoopStarted) {
            return;
        }

        pingLoopStarted = true;
        pingService = Executors.newSingleThreadScheduledExecutor();
        pingService.scheduleAtFixedRate(() -> {
            try {
                sendPing();
            } catch (IOException e) {
                shutdown("ping loop io error");
                try {
                    channel.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }, pingIntervalSeconds, pingIntervalSeconds, TimeUnit.SECONDS);
    }

    private synchronized void sendPing() throws IOException {
        if (!channel.isOpen()) {
            shutdown();
            return;
        }

        if (outstandingPingNonce != null) {
            System.out.println("closing channel due to missed pong for nonce " + outstandingPingNonce);
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
        System.out.println("sent ping with nonce " + nonce);
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
        if (pingService != null) {
            pingService.shutdownNow();
            pingService = null;
        }
        outstandingPingNonce = null;
        notifyConnectionClosed(reason);
    }

    private void dispatchTransaction() {
        RecSyncTransaction transaction = new RecSyncTransaction(
                sourceAddress(),
                List.copyOf(recordsToAdd),
                List.copyOf(recordsToDelete),
                List.copyOf(infoMessages),
                Map.copyOf(clientInfos));
        processorDispatcher.dispatchCommit(transaction);
        recordsToAdd.clear();
        recordsToDelete.clear();
        infoMessages.clear();
        clientInfos.clear();
    }

    private void notifyConnectionClosed(String reason) {
        if (connectionClosedNotified) {
            return;
        }
        connectionClosedNotified = true;
        processorDispatcher.dispatchConnectionClosed(new ConnectionClosedEvent(sourceAddress(), reason));
    }

    private String sourceAddress() {
        try {
            return String.valueOf(channel.getRemoteAddress());
        } catch (IOException e) {
            return "unknown";
        }
    }
}

