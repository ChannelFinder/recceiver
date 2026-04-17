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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.phoebus.recceiver.spi.RecSyncProcessorDispatcher;
import org.phoebus.recceiver.spi.RecSyncProcessorRegistry;

@Configuration
@ComponentScan(basePackages = {"org.phoebus.recceiver"})
@PropertySource(value = "classpath:application.properties")
@SpringBootApplication
public class RecceiverApplication implements CommandLineRunner {

    private static final Logger log = Logger.getLogger(RecceiverApplication.class.getName());

    @Value("${announceInterval:15.0}")
    private double announceInterval;
    @Value("${tcptimeout:15.0}")
    private double tcptimeout;
    @Value("${commitInterval:5.0}")
    private double commitInterval;
    @Value("${commitSizeLimit:0}")
    private int commitSizeLimit;
    @Value("${maxActive:20}")
    private int maxActive;

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
        processorDispatcher = RecSyncProcessorRegistry.loadDispatcher();

        Random rand = new Random();
        serverKey = rand.nextInt();

        Selector selector = Selector.open();
        // Create the TCP listener
        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress("localhost", 0));
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);

        InetSocketAddress localAddress = (InetSocketAddress) serverSocket.getLocalAddress();
        int port = localAddress.getPort();

        log.info("Listening on : " + port);

        RecceiverUDPAnnouncer announcer = new RecceiverUDPAnnouncer(serverKey, localAddress);
        udpAnnouncerService.scheduleAtFixedRate(announcer, 0, 15, TimeUnit.SECONDS);
        log.info("started announcing...");


        while (true) {
            selector.select();
            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            Iterator<SelectionKey> iter = selectedKeys.iterator();
            while (iter.hasNext()) {

                SelectionKey key = iter.next();
                System.out.println(key.isValid() + " " + key.channel());

                if (key.isAcceptable()) {
                    register(serverSocket);
                    log.info(selector + " " + serverSocket);
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
            MessageProcessor p = new MessageProcessor(client, Math.max(1L, Math.round(tcptimeout)), serverKey, processorDispatcher);
            try {
                while (client.isOpen() && client.isConnected()) {
                    p.process();
                }
            } catch (IOException e) {
                try {
                    client.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
                e.printStackTrace();
            } finally {
                p.shutdown();
            }

        });
    }

    @Bean
    public CountDownLatch closeLatch() {
        return new CountDownLatch(1);
    }

}
