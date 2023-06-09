package org.phoebus.recceiver;

import java.nio.channels.SocketChannel;

public class RecceiverSocket {

    private final SocketChannel client;
    // uploaded? connected? ping-pong
    private String status = "starting";

    private RecceiverSocket(SocketChannel client) {
        this.client = client;
    }

    static RecceiverSocket of(SocketChannel client){
        return new RecceiverSocket(client);
    }

    public SocketChannel getClient() {
        return client;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
