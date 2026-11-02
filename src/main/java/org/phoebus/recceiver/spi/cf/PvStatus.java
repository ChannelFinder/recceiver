package org.phoebus.recceiver.spi.cf;

/**
 * Values recceiver writes to a channel's {@code pvStatus} property in ChannelFinder.
 */
public enum PvStatus {
    /** The IOC publishing the channel is connected. */
    ACTIVE("Active"),
    /** The IOC disconnected, the record was removed, or recceiver cleaned up on startup. */
    INACTIVE("Inactive"),
    /** Recceiver shut down; the IOC's actual state is not known. */
    UNKNOWN("Unknown");

    private final String value;

    PvStatus(String value) {
        this.value = value;
    }

    /** @return the string stored in ChannelFinder for this status. */
    public String value() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}