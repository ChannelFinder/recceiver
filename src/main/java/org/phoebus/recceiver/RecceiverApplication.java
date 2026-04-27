package org.phoebus.recceiver;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.event.ContextClosedEvent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.phoebus.recceiver.spi.RecSyncProcessorDispatcher;
import org.phoebus.recceiver.spi.RecSyncProcessorRegistry;

@Configuration
@ComponentScan(basePackages = {"org.phoebus.recceiver"})
@PropertySource(value = "classpath:application.properties")
@SpringBootApplication
public class RecceiverApplication implements CommandLineRunner {

    private static final Logger log = Logger.getLogger(RecceiverApplication.class.getName());

    /** Network address the TCP listener binds to (default 0.0.0.0 = all interfaces). */
    @Value("${tcpBindAddress:0.0.0.0}")
    private String tcpBindAddress;

    /** TCP port to listen on (default 0 = OS-assigned ephemeral port). */
    @Value("${tcpPort:0}")
    private int tcpPort;

    /** TCP idle timeout in seconds (default 15 s). A missed pong closes the connection. */
    @Value("${tcptimeout:15.0}")
    private double tcptimeout;

    /** Size in bytes of the TCP read buffer per connection (default 4096, minimum 8). */
    @Value("${readBufferSize:4096}")
    private int readBufferSize;

    /** Destination address for UDP beacon packets (default 255.255.255.255 limited broadcast). */
    @Value("${udpBroadcastAddress:255.255.255.255}")
    private String udpBroadcastAddress;

    /** UDP port RecSync clients listen on for discovery beacons (default 5049). */
    @Value("${udpBroadcastPort:5049}")
    private int udpBroadcastPort;

    /** Seconds between UDP beacon broadcasts (default 15 s). */
    @Value("${announceInterval:15.0}")
    private double announceInterval;

    /** Seconds between automatic transaction commits (default 5 s). 0 = commit on every upload-complete. */
    @Value("${commitInterval:5.0}")
    private double commitInterval;

    /** Max record changes before forcing an early commit (default 0 = unlimited). */
    @Value("${commitSizeLimit:0}")
    private int commitSizeLimit;

    @Value("${recceiver.cf.enabled:false}")
    private String cfEnabled;

    @Value("${recceiver.cf.service-url:}")
    private String cfServiceUrl;

    @Value("${recceiver.cf.username:}")
    private String cfUsername;

    @Value("${recceiver.cf.password:}")
    private String cfPassword;

    @Value("${recceiver.cf.delete-on-clean:false}")
    private String cfDeleteOnClean;



    private static ConfigurableApplicationContext context;

    private final ScheduledExecutorService udpAnnouncerService = Executors.newScheduledThreadPool(1);
    private static int serverKey;
    private RecSyncProcessorDispatcher processorDispatcher;

    public static void main(String[] args) throws InterruptedException {
        context = SpringApplication.run(RecceiverApplication.class, args);
        log.info("Starting Recceiver");

        context.addApplicationListener(event -> {
            log.fine(event.toString());
            if (event instanceof ContextClosedEvent) {
                log.info("Close event.. need to cleanup and shutdown");
            }
        });

        final CountDownLatch closeLatch = context.getBean(CountDownLatch.class);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> closeLatch.countDown()));
        closeLatch.await();
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Starting running");
        processorDispatcher = RecSyncProcessorRegistry.loadDispatcher(buildProcessorOptions());

        Random rand = new Random();
        serverKey = rand.nextInt();

        Selector selector = Selector.open();
        // Create the TCP listener
        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress(tcpBindAddress, tcpPort));
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);

        InetSocketAddress localAddress = (InetSocketAddress) serverSocket.getLocalAddress();
        int port = localAddress.getPort();

        log.info("Listening on : " + tcpBindAddress + ":" + port);

        RecceiverUDPAnnouncer announcer = new RecceiverUDPAnnouncer(serverKey, localAddress, udpBroadcastAddress, udpBroadcastPort);
        udpAnnouncerService.scheduleAtFixedRate(announcer, 0, Math.round(announceInterval), TimeUnit.SECONDS);
        log.info("started announcing...");


        while (true) {
            selector.select();
            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            Iterator<SelectionKey> iter = selectedKeys.iterator();
            while (iter.hasNext()) {

                SelectionKey key = iter.next();
                if (log.isLoggable(Level.FINER)) {
                    log.finer("selection key valid=" + key.isValid() + " channel=" + key.channel());
                }

                if (key.isAcceptable()) {
                    register(serverSocket);
                    log.finer("accepted connection via selector=" + selector + " server=" + serverSocket);
                }
                iter.remove();
            }
        }

    }

    public static final char SERVER_GREET_MSG_ID = 0x8001;
    public static final char PING_MSG_ID = 0x8002;


    private void register(ServerSocketChannel serverSocket)
            throws IOException {

        SocketChannel client = serverSocket.accept();
        client.configureBlocking(true);

        // Run each client handler on its own virtual thread.
        Thread.ofVirtual().name("recceiver-client-", 0).start(() -> {
            MessageProcessor p = new MessageProcessor(client, Math.max(1L, Math.round(tcptimeout)), serverKey, processorDispatcher, readBufferSize);
            try {
                while (client.isOpen() && client.isConnected()) {
                    p.process();
                }
            } catch (IOException e) {
                try {
                    client.close();
                } catch (IOException ex) {
                    log.log(Level.WARNING, "Failed to close client channel", ex);
                }
                log.log(Level.WARNING, "Connection processing failed", e);
            } finally {
                p.shutdown();
            }

        });
    }

    private Map<String, String> buildProcessorOptions() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("recceiver.cf.enabled", cfEnabled);
        options.put("recceiver.cf.service-url", cfServiceUrl);
        options.put("recceiver.cf.username", cfUsername);
        options.put("recceiver.cf.password", cfPassword);
        options.put("recceiver.cf.delete-on-clean", cfDeleteOnClean);
        return options;
    }

    @Bean
    public CountDownLatch closeLatch() {
        return new CountDownLatch(1);
    }

}
