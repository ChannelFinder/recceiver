package org.phoebus.recceiver;

public record ConnectionClosedEvent(String sourceAddress, String reason) {
}

