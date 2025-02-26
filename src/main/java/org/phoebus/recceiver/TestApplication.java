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

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

@Configuration
@ComponentScan(basePackages = {"org.phoebus.recceiver"})
@PropertySource(value = "classpath:application.properties")
@SpringBootApplication
public class TestApplication implements CommandLineRunner {

    private static final Logger log = Logger.getLogger(TestApplication.class.getName());


    private static ConfigurableApplicationContext context;

    private final ScheduledExecutorService udpAnnouncerService = Executors.newScheduledThreadPool(1);
    private static int serverKey;

    public static void main(String[] args) throws InterruptedException {
        context = SpringApplication.run(TestApplication.class, args);
        log.info("Starting Test Application");

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

    private AsynchronousServerSocketChannel serverChannel;
    private Future<AsynchronousSocketChannel> acceptResult;
    private AsynchronousSocketChannel clientChannel;

    @Override
    public void run(String... args) throws Exception {
        log.info("Starting running");
        serverChannel = AsynchronousServerSocketChannel.open();
        InetSocketAddress hostAddress = new InetSocketAddress("localhost", 4999);
        serverChannel.bind(hostAddress);
        acceptResult = serverChannel.accept();

        runServer();
    }

    public void runServer() {
        try {
            clientChannel = acceptResult.get();
            if ((clientChannel != null) && (clientChannel.isOpen())) {
                while (true) {

                    ByteBuffer buffer = ByteBuffer.allocate(32);
                    Future<Integer> readResult = clientChannel.read(buffer);

                    // do some computation

                    readResult.get();

                    buffer.flip();
                    String message = new String(buffer.array()).trim();
                    if (message.equals("bye")) {
                        break; // while loop
                    }
                    buffer = ByteBuffer.wrap(new String(message).getBytes());
                    Future<Integer> writeResult = clientChannel.write(buffer);

                    // do some computation
                    writeResult.get();
                    buffer.clear();

                } // while()

                clientChannel.close();
                serverChannel.close();

            }
        } catch (InterruptedException | ExecutionException | IOException e) {
            e.printStackTrace();
        }
    }
}
