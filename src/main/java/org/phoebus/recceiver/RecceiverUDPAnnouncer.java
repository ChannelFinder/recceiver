package org.phoebus.recceiver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RecceiverUDPAnnouncer implements Runnable {
    private static final Logger udpLogger = Logger.getLogger(RecceiverUDPAnnouncer.class.getName());

    final DatagramChannel client;
    final InetSocketAddress address;
    final int key;

    public RecceiverUDPAnnouncer(int key, InetSocketAddress address) throws IOException {
        this.key = key;
        this.address = address;
        DatagramChannel datagramChannel = DatagramChannel.open();
        this.client = datagramChannel.bind(null);
    }

    @Override
    public void run() {
        this.client.isConnected();
        InetSocketAddress serverAddress = new InetSocketAddress("255.255.255.255", 5049);

        try {

            ByteBuffer announcerMsgBuffer;
            announcerMsgBuffer = ByteBuffer.allocate(16);
            announcerMsgBuffer.clear();
            announcerMsgBuffer.put("RC".getBytes(StandardCharsets.US_ASCII));
            announcerMsgBuffer.put((byte) 0);
            announcerMsgBuffer.put((byte) 0);
            announcerMsgBuffer.put(this.address.getAddress().getAddress(), 0, 4);
            announcerMsgBuffer.putShort((short)this.address.getPort());
            announcerMsgBuffer.put((byte) 0);
            announcerMsgBuffer.put((byte) 0);
            announcerMsgBuffer.putInt(key);
            announcerMsgBuffer.flip();
            udpLogger.info("sending out announcement at : " + Instant.now());
            client.send(announcerMsgBuffer, serverAddress);
        } catch (IOException e) {
            udpLogger.log(Level.SEVERE, e.getMessage(), e);
        }
    }
}
