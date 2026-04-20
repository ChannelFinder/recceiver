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
    /** Destination address for UDP beacon packets (e.g. 255.255.255.255 or a subnet broadcast). */
    final String broadcastAddress;
    /** UDP port RecSync clients listen on for discovery beacons. */
    final int broadcastPort;

    public RecceiverUDPAnnouncer(int key, InetSocketAddress address, String broadcastAddress, int broadcastPort) throws IOException {
        this.key = key;
        this.address = address;
        this.broadcastAddress = broadcastAddress;
        this.broadcastPort = broadcastPort;
        DatagramChannel datagramChannel = DatagramChannel.open();
        this.client = datagramChannel.bind(null);
    }

    @Override
    public void run() {
        this.client.isConnected();
        InetSocketAddress serverAddress = new InetSocketAddress(broadcastAddress, broadcastPort);

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
            udpLogger.fine("sending out announcement at: " + Instant.now());
            client.send(announcerMsgBuffer, serverAddress);
        } catch (IOException e) {
            udpLogger.log(Level.SEVERE, e.getMessage(), e);
        }
    }
}
