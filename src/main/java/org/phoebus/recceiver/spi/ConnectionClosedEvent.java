package org.phoebus.recceiver.spi;

public record ConnectionClosedEvent(String sourceAddress, String reason) {
}

