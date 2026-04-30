package org.phoebus.recceiver;

import org.junit.jupiter.api.Test;
import org.phoebus.recceiver.spi.AddInfoMessage;
import org.phoebus.recceiver.spi.AddRecordMessage;
import org.phoebus.recceiver.spi.DelRecordMessage;
import org.phoebus.recceiver.spi.RecSyncProcessor;
import org.phoebus.recceiver.spi.RecSyncTransaction;
import org.phoebus.recceiver.spi.cf.CFRecSyncProcessor;
import org.phoebus.recceiver.spi.cf.ChannelFinderChannel;
import org.phoebus.recceiver.spi.cf.ChannelFinderStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
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
    void initEnsuresAliasPropertyWhenAliasSupportEnabled() {
        TestStore store = new TestStore();
        TestProcessor processor = new TestProcessor(store);

        RecSyncProcessor initialized = processor.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder",
                "recceiver.cf.alias-enabled", "true"
        ));

        assertNotNull(initialized);
        assertTrue(store.ensuredProperties.contains(CFRecSyncProcessor.ALIAS_PROPERTY));
        assertEquals(11, store.ensuredProperties.size());
    }

    @Test
    void initEnsuresWhitelistedInfoTagProperties() {
        TestStore store = new TestStore();
        TestProcessor processor = new TestProcessor(store);

        RecSyncProcessor initialized = processor.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder",
                "recceiver.cf.infotags", "archive device"
        ));

        assertNotNull(initialized);
        assertTrue(store.ensuredProperties.contains("archive"));
        assertTrue(store.ensuredProperties.contains("device"));
        assertEquals(12, store.ensuredProperties.size());
    }

    @Test
    void initEnsuresEnvironmentVariableMappedProperties() {
        TestStore store = new TestStore();
        TestProcessor processor = new TestProcessor(store);

        RecSyncProcessor initialized = processor.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder",
                "recceiver.cf.environment-vars",
                "ENGINEER:Engineer,EPICS_BASE:EpicsVersion,PWD:WorkingDirectory"
        ));

        assertNotNull(initialized);
        assertTrue(store.ensuredProperties.contains("Engineer"));
        assertTrue(store.ensuredProperties.contains("EpicsVersion"));
        assertTrue(store.ensuredProperties.contains("WorkingDirectory"));
        assertEquals(13, store.ensuredProperties.size());
    }

    @Test
    void initFailsOnMalformedEnvironmentVariableMapping() {
        TestStore store = new TestStore();
        TestProcessor processor = new TestProcessor(store);

        assertThrows(IllegalArgumentException.class, () -> processor.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder",
                "recceiver.cf.environment-vars", "ENGINEER"
        )));
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

    @Test
    void commitPublishesAliasChannelsWhenEnabled() {
        TestStore store = new TestStore();
        TestProcessor processor = new TestProcessor(store);
        processor.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder",
                "recceiver.cf.alias-enabled", "true"
        ));

        RecSyncTransaction transaction = new RecSyncTransaction(
                "/127.0.0.1:5070",
                List.of(
                        new AddRecordMessage(10, 0, 3, 6, "ai", "pv:one"),
                        new AddRecordMessage(10, 1, 3, 12, "ai", "pv:one:alias")
                ),
                List.of(),
                List.of(),
                Map.of(
                        CFRecSyncProcessor.IOCNAME_KEY, "ioc-a",
                        CFRecSyncProcessor.HOSTNAME_KEY, "host-a",
                        CFRecSyncProcessor.ENGINEER_KEY, "alice"
                ),
                true,
                true
        );

        processor.commit(transaction);

        assertEquals(2, store.updatedChannels.size());
        ChannelFinderChannel primary = store.updatedChannels.stream()
                .filter(channel -> "pv:one".equals(channel.name()))
                .findFirst()
                .orElseThrow();
        ChannelFinderChannel alias = store.updatedChannels.stream()
                .filter(channel -> "pv:one:alias".equals(channel.name()))
                .findFirst()
                .orElseThrow();

        assertEquals("alice", primary.owner());
        assertEquals("alice", alias.owner());
        assertEquals("pv:one", alias.property(CFRecSyncProcessor.ALIAS_PROPERTY));
        assertEquals("host-a", alias.property(CFRecSyncProcessor.HOST_NAME_PROPERTY));
        assertEquals("ioc-a", alias.property(CFRecSyncProcessor.IOC_NAME_PROPERTY));
        assertEquals("127.0.0.1:5070", alias.property(CFRecSyncProcessor.IOC_ID_PROPERTY));
        assertEquals(CFRecSyncProcessor.ACTIVE, alias.property(CFRecSyncProcessor.PV_STATUS_PROPERTY));
        assertEquals("ai", alias.property(CFRecSyncProcessor.RECORD_TYPE_PROPERTY));
    }

    @Test
    void commitPublishesOnlyWhitelistedInfoTags() {
        TestStore store = new TestStore();
        TestProcessor processor = new TestProcessor(store);
        processor.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder",
                "recceiver.cf.alias-enabled", "true",
                "recceiver.cf.infotags", "archive, device"
        ));

        RecSyncTransaction transaction = new RecSyncTransaction(
                "/127.0.0.1:5070",
                List.of(
                        new AddRecordMessage(10, 0, 3, 6, "ai", "pv:one"),
                        new AddRecordMessage(10, 1, 3, 12, "ai", "pv:one:alias")
                ),
                List.of(),
                List.of(
                        new AddInfoMessage(10, 7, 11, "archive", "MONITOR@0.1"),
                        new AddInfoMessage(10, 6, 7, "device", "demo-cf"),
                        new AddInfoMessage(10, 4, 3, "DESC", "abc"),
                        new AddInfoMessage(99, 7, 4, "archive", "lost")
                ),
                Map.of(
                        CFRecSyncProcessor.IOCNAME_KEY, "ioc-a",
                        CFRecSyncProcessor.HOSTNAME_KEY, "host-a",
                        CFRecSyncProcessor.ENGINEER_KEY, "alice"
                ),
                true,
                true
        );

        processor.commit(transaction);

        assertEquals(2, store.updatedChannels.size());
        ChannelFinderChannel primary = store.updatedChannels.stream()
                .filter(channel -> "pv:one".equals(channel.name()))
                .findFirst()
                .orElseThrow();
        ChannelFinderChannel alias = store.updatedChannels.stream()
                .filter(channel -> "pv:one:alias".equals(channel.name()))
                .findFirst()
                .orElseThrow();

        assertEquals("MONITOR@0.1", primary.property("archive"));
        assertEquals("demo-cf", primary.property("device"));
        assertNull(primary.property("DESC"));
        assertEquals("MONITOR@0.1", alias.property("archive"));
        assertEquals("demo-cf", alias.property("device"));
        assertNull(alias.property("DESC"));
        assertEquals("pv:one", alias.property(CFRecSyncProcessor.ALIAS_PROPERTY));
    }

    @Test
    void commitPublishesMappedEnvironmentVariableProperties() {
        TestStore store = new TestStore();
        TestProcessor processor = new TestProcessor(store);
        processor.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder",
                "recceiver.cf.alias-enabled", "true",
                "recceiver.cf.environment-vars",
                "ENGINEER:Engineer,EPICS_BASE:EpicsVersion,PWD:WorkingDirectory"
        ));

        RecSyncTransaction transaction = new RecSyncTransaction(
                "/127.0.0.1:5070",
                List.of(
                        new AddRecordMessage(10, 0, 3, 6, "ai", "pv:one"),
                        new AddRecordMessage(10, 1, 3, 12, "ai", "pv:one:alias")
                ),
                List.of(),
                List.of(),
                Map.of(
                        CFRecSyncProcessor.IOCNAME_KEY, "ioc-a",
                        CFRecSyncProcessor.HOSTNAME_KEY, "host-a",
                        CFRecSyncProcessor.ENGINEER_KEY, "alice",
                        "EPICS_BASE", "7.0.8",
                        "PWD", "/ioc/demo"
                ),
                true,
                true
        );

        processor.commit(transaction);

        assertEquals(2, store.updatedChannels.size());
        ChannelFinderChannel primary = store.updatedChannels.stream()
                .filter(channel -> "pv:one".equals(channel.name()))
                .findFirst()
                .orElseThrow();
        ChannelFinderChannel alias = store.updatedChannels.stream()
                .filter(channel -> "pv:one:alias".equals(channel.name()))
                .findFirst()
                .orElseThrow();

        assertEquals("alice", primary.property("Engineer"));
        assertEquals("7.0.8", primary.property("EpicsVersion"));
        assertEquals("/ioc/demo", primary.property("WorkingDirectory"));
        assertEquals("alice", alias.property("Engineer"));
        assertEquals("7.0.8", alias.property("EpicsVersion"));
        assertEquals("/ioc/demo", alias.property("WorkingDirectory"));
    }

    @Test
    void initParsesDeleteOnShutdownConfiguration() {
        TestStore store = new TestStore();
        TestProcessor processor = new TestProcessor(store);

        // Test default value (true)
        RecSyncProcessor initialized = processor.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder"
        ));
        assertNotNull(initialized);

        // Test explicit false value
        TestProcessor processorFalse = new TestProcessor(store);
        initialized = processorFalse.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder",
                "recceiver.cf.delete-on-shutdown", "false"
        ));
        assertNotNull(initialized);
    }

    @Test
    void registersIocInShutdownRegistryOnInitialCommit() {
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
                        CFRecSyncProcessor.HOSTNAME_KEY, "host-a"
                ),
                true,  // initial = true
                true   // connected = true
        );

        processor.commit(transaction);

        // Verify that the IOC was registered
        assertEquals(1, processor.getIocRegistrySize());
    }

    @Test
    void destroyMarksChannelsInactiveWhenDeleteOnShutdownFalse() {
        TestStore store = new TestStore();
        store.findByPropertyResult = Map.of(
                "pv:one", new ChannelFinderChannel("pv:one", "owner1", Map.of(
                        CFRecSyncProcessor.IOC_ID_PROPERTY, "127.0.0.1:5070",
                        CFRecSyncProcessor.PV_STATUS_PROPERTY, CFRecSyncProcessor.ACTIVE
                ))
        );

        TestProcessor processor = new TestProcessor(store);
        processor.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder",
                "recceiver.cf.delete-on-shutdown", "false"
        ));

        // Register an IOC
        RecSyncTransaction transaction = new RecSyncTransaction(
                "/127.0.0.1:5070",
                List.of(new AddRecordMessage(10, 0, 3, 6, "ai", "pv:one")),
                List.of(),
                List.of(),
                Map.of(
                        CFRecSyncProcessor.IOCNAME_KEY, "ioc-a",
                        CFRecSyncProcessor.HOSTNAME_KEY, "host-a"
                ),
                true,
                true
        );
        processor.commit(transaction);

        store.updatedChannels.clear();

        // Destroy should mark channels as inactive, not delete them
        processor.destroy();

        // Verify channels were updated (not deleted)
        assertEquals(1, store.updatedChannels.size());
        ChannelFinderChannel updated = store.updatedChannels.get(0);
        assertEquals(CFRecSyncProcessor.INACTIVE, updated.property(CFRecSyncProcessor.PV_STATUS_PROPERTY));
        assertEquals(0, store.deletedChannelNames.size());
    }

    @Test
    void destroyDeletesChannelsWhenDeleteOnShutdownTrue() {
        TestStore store = new TestStore();
        store.findByPropertyResult = Map.of(
                "pv:one", new ChannelFinderChannel("pv:one", "owner1", Map.of(
                        CFRecSyncProcessor.IOC_ID_PROPERTY, "127.0.0.1:5070",
                        CFRecSyncProcessor.PV_STATUS_PROPERTY, CFRecSyncProcessor.ACTIVE
                ))
        );

        TestProcessor processor = new TestProcessor(store);
        processor.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder",
                "recceiver.cf.delete-on-shutdown", "true"
        ));

        // Register an IOC
        RecSyncTransaction transaction = new RecSyncTransaction(
                "/127.0.0.1:5070",
                List.of(new AddRecordMessage(10, 0, 3, 6, "ai", "pv:one")),
                List.of(),
                List.of(),
                Map.of(
                        CFRecSyncProcessor.IOCNAME_KEY, "ioc-a",
                        CFRecSyncProcessor.HOSTNAME_KEY, "host-a"
                ),
                true,
                true
        );
        processor.commit(transaction);

        store.updatedChannels.clear();

        // Destroy should delete channels
        processor.destroy();

        // Verify channels were deleted, not updated
        assertEquals(0, store.updatedChannels.size());
        assertEquals(1, store.deletedChannelNames.size());
        assertTrue(store.deletedChannelNames.contains("pv:one"));
    }

    @Test
    void commitDeletesPrimaryAndAliasWhenRecordIsDeletedAndDeleteOnCleanTrue() {
        TestStore store = new TestStore();
        TestProcessor processor = new TestProcessor(store);
        processor.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder",
                "recceiver.cf.alias-enabled", "true",
                "recceiver.cf.delete-on-clean", "true"
        ));

        RecSyncTransaction addTransaction = new RecSyncTransaction(
                "/127.0.0.1:5070",
                List.of(
                        new AddRecordMessage(10, 0, 3, 6, "ai", "pv:one"),
                        new AddRecordMessage(10, 1, 0, 12, "", "pv:one:alias")
                ),
                List.of(),
                List.of(),
                Map.of(
                        CFRecSyncProcessor.IOCNAME_KEY, "ioc-a",
                        CFRecSyncProcessor.HOSTNAME_KEY, "host-a"
                ),
                true,
                true
        );
        processor.commit(addTransaction);

        store.deletedChannelNames.clear();
        store.findByNamesResult = Map.of(
                "pv:one", new ChannelFinderChannel("pv:one", "owner", Map.of()),
                "pv:one:alias", new ChannelFinderChannel("pv:one:alias", "owner", Map.of())
        );

        RecSyncTransaction deleteTransaction = new RecSyncTransaction(
                "/127.0.0.1:5070",
                List.of(),
                List.of(new DelRecordMessage(10)),
                List.of(),
                Map.of(),
                false,
                true
        );
        processor.commit(deleteTransaction);

        assertEquals(2, store.deletedChannelNames.size());
        assertTrue(store.deletedChannelNames.contains("pv:one"));
        assertTrue(store.deletedChannelNames.contains("pv:one:alias"));
    }

    @Test
    void commitMarksRecordInactiveWhenDeletedAndDeleteOnCleanFalse() {
        TestStore store = new TestStore();
        TestProcessor processor = new TestProcessor(store);
        processor.init("cf", Map.of(
                "recceiver.cf.enabled", "true",
                "recceiver.cf.service-url", "http://localhost:9090/ChannelFinder",
                "recceiver.cf.delete-on-clean", "false"
        ));

        RecSyncTransaction addTransaction = new RecSyncTransaction(
                "/127.0.0.1:5070",
                List.of(new AddRecordMessage(10, 0, 3, 6, "ai", "pv:one")),
                List.of(),
                List.of(),
                Map.of(
                        CFRecSyncProcessor.IOCNAME_KEY, "ioc-a",
                        CFRecSyncProcessor.HOSTNAME_KEY, "host-a"
                ),
                true,
                true
        );
        processor.commit(addTransaction);

        store.updatedChannels.clear();
        store.findByNamesResult = Map.of(
                "pv:one", new ChannelFinderChannel("pv:one", "owner", Map.of(
                        CFRecSyncProcessor.PV_STATUS_PROPERTY, CFRecSyncProcessor.ACTIVE,
                        CFRecSyncProcessor.TIME_PROPERTY, "old-time"
                ))
        );

        RecSyncTransaction deleteTransaction = new RecSyncTransaction(
                "/127.0.0.1:5070",
                List.of(),
                List.of(new DelRecordMessage(10)),
                List.of(),
                Map.of(),
                false,
                true
        );
        processor.commit(deleteTransaction);

        assertEquals(1, store.updatedChannels.size());
        ChannelFinderChannel updated = store.updatedChannels.get(0);
        assertEquals("pv:one", updated.name());
        assertEquals(CFRecSyncProcessor.INACTIVE, updated.property(CFRecSyncProcessor.PV_STATUS_PROPERTY));
        assertNotNull(updated.property(CFRecSyncProcessor.TIME_PROPERTY));
        assertEquals(0, store.deletedChannelNames.size());
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

        // Test helper to check IOC registry size
        int getIocRegistrySize() {
            return iocRegistrySize();
        }
    }

    private static final class TestStore implements ChannelFinderStore {
        private String ensuredOwner;
        private Set<String> ensuredProperties = Set.of();
        private final List<ChannelFinderChannel> updatedChannels = new ArrayList<>();
        private final List<String> deletedChannelNames = new ArrayList<>();
        private Map<String, ChannelFinderChannel> findByNamesResult = Map.of();
        private Map<String, ChannelFinderChannel> findByPropertyResult = Map.of();

        @Override
        public Map<String, ChannelFinderChannel> findByNames(Set<String> channelNames) {
            Map<String, ChannelFinderChannel> result = new LinkedHashMap<>();
            for (String channelName : channelNames) {
                ChannelFinderChannel channel = findByNamesResult.get(channelName);
                if (channel != null) {
                    result.put(channelName, channel);
                }
            }
            return Map.copyOf(result);
        }

        @Override
        public Map<String, ChannelFinderChannel> findByProperty(String propertyName, String propertyValue) {
            return findByPropertyResult;
        }

        @Override
        public void updateChannels(Collection<ChannelFinderChannel> channels) {
            updatedChannels.addAll(channels);
        }

        @Override
        public void deleteChannels(Collection<String> channelNames) {
            deletedChannelNames.addAll(channelNames);
        }

        @Override
        public void ensureProperties(Set<String> propertyNames, String owner) {
            this.ensuredOwner = owner;
            this.ensuredProperties = Set.copyOf(new LinkedHashSet<>(propertyNames));
        }
    }
}
