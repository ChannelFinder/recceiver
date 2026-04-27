package org.phoebus.recceiver;

import org.junit.jupiter.api.Test;
import org.phoebus.recceiver.spi.AddRecordMessage;
import org.phoebus.recceiver.spi.RecSyncProcessor;
import org.phoebus.recceiver.spi.RecSyncTransaction;
import org.phoebus.recceiver.spi.cf.CFRecSyncProcessor;
import org.phoebus.recceiver.spi.cf.ChannelFinderChannel;
import org.phoebus.recceiver.spi.cf.ChannelFinderStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CFRecSyncProcessorTest {

    @Test
    void initReturnsNullWhenDisabled() {
        TestStore store = new TestStore();
        TestProcessor processor = new TestProcessor(store);

        RecSyncProcessor initialized = processor.init("cf", Map.of());

        assertNull(initialized);
        assertNull(store.ensuredOwner);
        assertTrue(store.ensuredProperties.isEmpty());
    }

    @Test
    void initEnsuresRequiredPropertiesWithDefaultOwner() {
        TestStore store = new TestStore();
        TestProcessor processor = new TestProcessor(store);

        RecSyncProcessor initialized = processor.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder"
        ));

        assertNotNull(initialized);
        assertEquals(CFRecSyncProcessor.DEFAULT_OWNER, store.ensuredOwner);
        assertEquals(Set.of(
                CFRecSyncProcessor.HOST_NAME_PROPERTY,
                CFRecSyncProcessor.IOC_NAME_PROPERTY,
                CFRecSyncProcessor.PV_STATUS_PROPERTY,
                CFRecSyncProcessor.TIME_PROPERTY,
                CFRecSyncProcessor.IOC_ID_PROPERTY,
                CFRecSyncProcessor.IOC_IP_PROPERTY,
                CFRecSyncProcessor.RECCEIVER_ID_PROPERTY,
                CFRecSyncProcessor.RECORD_TYPE_PROPERTY,
                CFRecSyncProcessor.CA_PORT_PROPERTY,
                CFRecSyncProcessor.PVA_PORT_PROPERTY
        ), store.ensuredProperties);
    }

    @Test
    void initEnsuresRequiredPropertiesWithConfiguredOwner() {
        TestStore store = new TestStore();
        TestProcessor processor = new TestProcessor(store);

        RecSyncProcessor initialized = processor.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder",
                "recceiver.cf.username", "cfuser",
                "recceiver.cf.password", "secret"
        ));

        assertNotNull(initialized);
        assertEquals("cfuser", store.ensuredOwner);
        assertEquals(10, store.ensuredProperties.size());
    }

    @Test
    void initFailsWhenOnlyOneCredentialProvided() {
        TestStore store = new TestStore();
        TestProcessor processor = new TestProcessor(store);
        Map<String, String> options = Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder",
                "recceiver.cf.username", "cfuser"
        );

        assertThrows(IllegalArgumentException.class, () -> processor.init("cf", options));
    }

    @Test
    void commitPublishesStandardPropertiesFromTransaction() {
        TestStore store = new TestStore();
        TestProcessor processor = new TestProcessor(store);
        processor.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder"
        ));

        RecSyncTransaction transaction = new RecSyncTransaction(
                "/127.0.0.1:5070",
                List.of(new AddRecordMessage(10, 0, 3, 6, "ai", "pv:one")),
                List.of(),
                List.of(),
                Map.of(
                        CFRecSyncProcessor.IOCNAME_KEY, "ioc-a",
                        CFRecSyncProcessor.HOSTNAME_KEY, "host-a",
                        CFRecSyncProcessor.ENGINEER_KEY, "alice",
                        CFRecSyncProcessor.CA_PORT_ENV_KEY, "5064",
                        CFRecSyncProcessor.PVA_PORT_ENV_KEY, "5075"
                ),
                true,
                true
        );

        processor.commit(transaction);

        assertEquals(1, store.updatedChannels.size());
        ChannelFinderChannel channel = store.updatedChannels.getFirst();
        assertEquals("pv:one", channel.name());
        assertEquals("alice", channel.owner());
        assertEquals("host-a", channel.property(CFRecSyncProcessor.HOST_NAME_PROPERTY));
        assertEquals("ioc-a", channel.property(CFRecSyncProcessor.IOC_NAME_PROPERTY));
        assertEquals(CFRecSyncProcessor.ACTIVE, channel.property(CFRecSyncProcessor.PV_STATUS_PROPERTY));
        assertEquals("127.0.0.1:5070", channel.property(CFRecSyncProcessor.IOC_ID_PROPERTY));
        assertEquals("127.0.0.1", channel.property(CFRecSyncProcessor.IOC_IP_PROPERTY));
        assertEquals("ai", channel.property(CFRecSyncProcessor.RECORD_TYPE_PROPERTY));
        assertEquals("5064", channel.property(CFRecSyncProcessor.CA_PORT_PROPERTY));
        assertEquals("5075", channel.property(CFRecSyncProcessor.PVA_PORT_PROPERTY));
        assertNotNull(channel.property(CFRecSyncProcessor.TIME_PROPERTY));
        assertFalse(channel.property(CFRecSyncProcessor.TIME_PROPERTY).isBlank());
        assertNotNull(channel.property(CFRecSyncProcessor.RECCEIVER_ID_PROPERTY));
        assertFalse(channel.property(CFRecSyncProcessor.RECCEIVER_ID_PROPERTY).isBlank());
    }

    private static final class TestProcessor extends CFRecSyncProcessor {
        private final ChannelFinderStore store;

        private TestProcessor(ChannelFinderStore store) {
            this.store = store;
        }

        @Override
        protected ChannelFinderStore createStore(String serviceUrl, String username, String password) {
            return store;
        }
    }

    private static final class TestStore implements ChannelFinderStore {
        private String ensuredOwner;
        private Set<String> ensuredProperties = Set.of();
        private final List<ChannelFinderChannel> updatedChannels = new ArrayList<>();

        @Override
        public Map<String, ChannelFinderChannel> findByNames(Set<String> channelNames) {
            return Map.of();
        }

        @Override
        public void updateChannels(Collection<ChannelFinderChannel> channels) {
            updatedChannels.addAll(channels);
        }

        @Override
        public void deleteChannels(Collection<String> channelNames) {
            // not needed for this test store
        }

        @Override
        public void ensureProperties(Set<String> propertyNames, String owner) {
            this.ensuredOwner = owner;
            this.ensuredProperties = Set.copyOf(new LinkedHashSet<>(propertyNames));
        }
    }
}
