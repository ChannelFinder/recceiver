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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

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

    ConcurrentHashMap<String, Processor> channelMap;

    @Override
    public void run(String... args) throws Exception {
        log.info("Starting running");
        channelMap = new ConcurrentHashMap<>();

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
                    register(selector, serverSocket);
                    log.info(selector + " " + serverSocket);
                }
                if (key.isReadable()) {
                    SocketChannel client = (SocketChannel) key.channel();

                    if (channelMap.containsKey(client.getRemoteAddress().toString())) {
                        System.out.println("Reading from known remote address : " + client.getRemoteAddress().toString());
                        channelMap.get(client.getRemoteAddress().toString()).process();
                    } else {
                        System.out.println("Reading from new remote address : " + client.getRemoteAddress().toString());
                        channelMap.put(client.getRemoteAddress().toString(), new Processor(client));
                        channelMap.get(client.getRemoteAddress().toString()).process();
                    }
                }
                iter.remove();
            }
        }

    }

    public static final char SERVER_GREET_MSG_ID = 0x8001;
    public static final char PING_MSG_ID = 0x8002;

    private void decodeMessage(SocketChannel client) throws IOException {
        ByteBuffer readBuffer = ByteBuffer.allocate(1024);
        client.read(readBuffer);
        readBuffer.flip();

        // Decode the header
        byte[] idArray = new byte[2];
        readBuffer.get(idArray, 0, 2);
        //Charset charset = Charset.forName("US-ASCII");
        //StandardCharsets.US_ASCII.decode()
        String id = String.valueOf(StandardCharsets.US_ASCII.decode(ByteBuffer.wrap(idArray)).array());
        char msgId = readBuffer.getChar();
        int msgLen = readBuffer.getInt();

        System.out.println("processing message " + id + " " + String.format("%04x", (int) msgId) + " " + msgLen);
        if (!"RC".equals(id)) {
            // invalid id... ignore
            return;
        }

        switch (msgId) {
            case 0x0001:
                System.out.println("received 0001 " + readBuffer.remaining());
                char clientGreet = readBuffer.getChar();
                char clientGreetPadding = readBuffer.getChar();
                int returnedKey = readBuffer.getInt();

                // validate
                if (returnedKey == serverKey) {
                    // send the reply
                    ByteBuffer serverReply = ByteBuffer.allocate(9);
                    serverReply.put("RC".getBytes(StandardCharsets.US_ASCII));
                    serverReply.putChar(SERVER_GREET_MSG_ID);
                    serverReply.putInt(1);
                    serverReply.put((byte) 0x00);
                    serverReply.flip();
                    client.write(serverReply);
                    serverReply.clear();
                } else {

                }
                break;
            case 0x0002:
                System.out.println("received 0002");
                System.out.println(String.valueOf(StandardCharsets.US_ASCII.decode(readBuffer)));
                break;
            case 0x0003:
                System.out.println("received 0003");
                System.out.println(String.valueOf(StandardCharsets.US_ASCII.decode(readBuffer)));
                break;
            case 0x0004:
                System.out.println("received 0004");
                System.out.println(String.valueOf(StandardCharsets.US_ASCII.decode(readBuffer)));
                break;
            case 0x0005:
                System.out.println("received 0005");
                System.out.println(String.valueOf(StandardCharsets.US_ASCII.decode(readBuffer)));
                break;
            case 0x0006:
                System.out.println("received 0006 - Add info " + readBuffer.remaining());
                System.out.println(String.valueOf(StandardCharsets.US_ASCII.decode(readBuffer)));
//				try {
//					int recID = readBuffer.getInt();
//					int keyLength = readBuffer.get();
//					readBuffer.get();
//					int valueLength = readBuffer.getChar();
//					ByteBuffer keyBuffer = ByteBuffer.allocate(keyLength);
//					ByteBuffer valueBuffer = ByteBuffer.allocate(valueLength);
//					readBuffer.get(keyBuffer.array(),0,keyLength);
//					String key = String.valueOf(StandardCharsets.US_ASCII.decode(keyBuffer));
//					readBuffer.get(valueBuffer.array(),0,valueLength);
//					String value = String.valueOf(StandardCharsets.US_ASCII.decode(valueBuffer));
//					System.out.println("Add info : " + recID + " key: ("+keyLength+")"+key+" value: ("+valueLength+")" + value);
//				} catch (Exception e) {
//					System.out.println("failed to read info message because buffer size was 0");
//				}
//				readBuffer.clear();
                break;
        }

    }

    private static void register(Selector selector, ServerSocketChannel serverSocket)
            throws IOException {

        SocketChannel client = serverSocket.accept();
        client.configureBlocking(false);
        //client.register(selector, SelectionKey.OP_READ);

        // try with new thread
        Thread t = new Thread(() -> {
            Processor p = new Processor(client);
            while(true) {
                if( client.isConnected() ) {
                    try {
                        p.process();
                    } catch (IOException e) {
                        try {
                            client.close();
                        } catch (IOException ex) {
                            ex.printStackTrace();
                        }
                        e.printStackTrace();
                    }
                }
            }

        });
        t.start();
    }

    @Bean
    public CountDownLatch closeLatch() {
        return new CountDownLatch(1);
    }

    public static class Processor {
        private final SocketChannel channel;

        private boolean processingPartialMessage = false;

        public Processor(SocketChannel channel) {
            this.channel = channel;
        }

        ConcurrentLinkedQueue<Message> completeMessages = new ConcurrentLinkedQueue<>();
        Message partialMessage;
        ByteBuffer leftOver;

        public void process() throws IOException {
            System.out.println("..................................");
            ByteBuffer readBuffer = ByteBuffer.allocate(10);

            if(leftOver != null) {
                System.out.println("copying leftover : " + leftOver.remaining() + " " + DatatypeConverter.printHexBinary(leftOver.array()));
                leftOver.flip();
                readBuffer.put(leftOver);
                leftOver = null;
            }
            channel.read(readBuffer);
            System.out.println(DatatypeConverter.printHexBinary(readBuffer.array()));
            readBuffer.flip();
            System.out.println(DatatypeConverter.printHexBinary(readBuffer.array()) + " " + readBuffer.remaining());
            if(readBuffer.remaining() <= 0) {
                System.out.println("need to close");
                channel.close();
            }


            while (readBuffer.hasRemaining()) {
                System.out.println("remaining buffer is " + readBuffer.position() + " " + readBuffer.limit() + " " + readBuffer.remaining() + " " + processingPartialMessage + " " + partialMessage);
                // a brand new message
                if (!processingPartialMessage && readBuffer.remaining() >= 8) {
                    processingPartialMessage = true;
                    Message message = processHeader(readBuffer);
                    System.out.println("Processing a new message... Buffer size pre processing " + readBuffer.remaining());
                    if (readBuffer.remaining() >= message.msgLen) {
                        message.getMsgBody().put(readBuffer.array(), 0, message.msgLen);
                        message.getMsgBody().flip();
                        readBuffer.position(readBuffer.position() + message.msgLen);
                        // now we have a complete message put it in the queue
                        completeMessages.add(message);
                        processingPartialMessage = false;
                    } else {
                        System.out.println("PARTIAL message: " + readBuffer.remaining() + " available but need " + message.msgLen);
                        //System.out.println(String.valueOf(StandardCharsets.US_ASCII.decode(readBuffer)));
                        if (readBuffer.remaining() > 0) {
                            try {
                                System.out.println("before transfer : " + message.msgBody.remaining() + " : readbuffer " + readBuffer.remaining());
                                //readBuffer.get(message.getMsgBody().array(), 0, readBuffer.remaining());
                                message.getMsgBody().put(readBuffer);
                                System.out.println("after transfer : " + message.msgBody.remaining() + " : readbuffer " + readBuffer.remaining());
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            System.out.println("PARTIAL message is moved still expecting " + message.getRemainingBytes());
                        }
                        partialMessage = message;
                    }
                } else if (processingPartialMessage && readBuffer.hasRemaining()) {
                    System.out.println("Processing partial Message " + readBuffer.remaining() + " " + partialMessage.getRemainingBytes());
                    if (readBuffer.remaining() >= partialMessage.getRemainingBytes()) {
                        int remainingBytes = partialMessage.getRemainingBytes();
                        partialMessage.getMsgBody().put(readBuffer.array(), 0, remainingBytes);
                        readBuffer.position(readBuffer.position() + remainingBytes);
                        //readBuffer.get(partialMessage.getMsgBody().array(), partialMessage.getMsgBody().remaining(), partialMessage.getRemainingBytes());
                        partialMessage.getMsgBody().flip();
                        completeMessages.add(partialMessage);
                        partialMessage = null;
                        processingPartialMessage = false;
                    } else {
                        //readBuffer.get(partialMessage.getMsgBody().array(), partialMessage.getMsgBody().remaining(), readBuffer.remaining());
                        partialMessage.getMsgBody().put(readBuffer);
                    }
                } else {
                    System.out.println("Processing a new incomplete message... Buffer size pre processing " + readBuffer.position() + " " + readBuffer.remaining());
                    System.out.println(DatatypeConverter.printHexBinary(readBuffer.array()));
                    leftOver = ByteBuffer.allocate(readBuffer.remaining());
                    leftOver.put(readBuffer);
                    System.out.println("Processing a new incomplete message... Buffer size processing " + readBuffer.position() + " " + readBuffer.remaining());
                    System.out.println(DatatypeConverter.printHexBinary(readBuffer.array()));
                    break;
                }

                System.out.println("complete message count: " + completeMessages.size());
                // check for complete messages
                completeMessages.stream().forEach(message -> {
                    System.out.println("processing completed message " + message.id + " " + String.format("%04x", (int) message.msgId) + " " + message.msgLen);
                    switch (message.msgId) {
                        case 0x0001:
                            System.out.println("received 0001 " + message.msgBody.remaining());
                            char clientGreet = message.msgBody.getChar();
                            char clientGreetPadding = message.msgBody.getChar();
                            int returnedKey = message.msgBody.getInt();

                            // validate
                            if (returnedKey == serverKey) {
                                // send the reply
                                ByteBuffer serverReply = ByteBuffer.allocate(9);
                                serverReply.put("RC".getBytes(StandardCharsets.US_ASCII));
                                serverReply.putChar(SERVER_GREET_MSG_ID);
                                serverReply.putInt(1);
                                serverReply.put((byte) 0x00);
                                serverReply.flip();
                                try {
                                    channel.write(serverReply);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                serverReply.clear();
                            } else {

                            }
                            break;
                        case 0x0002:
                            System.out.println("received 0002");
                            System.out.println(String.valueOf(StandardCharsets.US_ASCII.decode(message.msgBody)));
                            break;
                        case 0x0003:
                            System.out.println("received 0003");
                            //System.out.println(String.valueOf(StandardCharsets.US_ASCII.decode(message.msgBody)));
                            try {
                                int recID = message.msgBody.getInt();
                                int aType = message.msgBody.get();
                                int rtLength = message.msgBody.get();
                                int rnLength = message.msgBody.getChar();

                                ByteBuffer recordTypeBuffer = ByteBuffer.allocate(rtLength);
                                ByteBuffer recordNameBuffer = ByteBuffer.allocate(rnLength);
                                message.msgBody.get(recordTypeBuffer.array(), 0, rtLength);
                                String recordType = String.valueOf(StandardCharsets.US_ASCII.decode(recordTypeBuffer));
                                message.msgBody.get(recordNameBuffer.array(), 0, rnLength);
                                String recordName = String.valueOf(StandardCharsets.US_ASCII.decode(recordNameBuffer));
                                System.out.println("Add record : " + recID + " recordType(" + rtLength + "): " + recordType + " value(" + rnLength + "): " + recordName);
                            } catch (Exception e) {
                                System.out.println("failed to read " + e.getMessage());
                            }
                            break;
                        case 0x0004:
                            System.out.println("received 0004 - DELETE" + String.valueOf(StandardCharsets.US_ASCII.decode(message.msgBody)));
                            break;
                        case 0x0005:
                            System.out.println("received 0005 - upload completed... start ping pong");
                            System.out.println(String.valueOf(StandardCharsets.US_ASCII.decode(message.msgBody)));
                            break;
                        case 0x0006:
                            System.out.println("received 0006 - Add info " + message.msgBody.remaining());
                            // System.out.println(String.valueOf(StandardCharsets.US_ASCII.decode(message.msgBody)));
                            // System.out.println(message.msgBody.remaining());
                            try {
                                int recID = message.msgBody.getInt();
                                int keyLength = message.msgBody.get();
                                message.msgBody.get();
                                int valueLength = message.msgBody.getChar();
                                ByteBuffer keyBuffer = ByteBuffer.allocate(keyLength);
                                ByteBuffer valueBuffer = ByteBuffer.allocate(valueLength);
                                message.msgBody.get(keyBuffer.array(), 0, keyLength);
                                String key = String.valueOf(StandardCharsets.US_ASCII.decode(keyBuffer));
                                message.msgBody.get(valueBuffer.array(), 0, valueLength);
                                String value = String.valueOf(StandardCharsets.US_ASCII.decode(valueBuffer));
                                System.out.println("Add info : " + recID + " key: (" + keyLength + ")" + key + " value: (" + valueLength + ")" + value);
                            } catch (Exception e) {
                                System.out.println("failed to read info message " + e.getMessage());
                            }
                            break;
                    }
                });
                completeMessages.clear();
            }

        }

        private Message processHeader(ByteBuffer readBuffer) {
            // Decode the header
            byte[] idArray = new byte[2];
            readBuffer.get(idArray, 0, 2);

            String id = String.valueOf(StandardCharsets.US_ASCII.decode(ByteBuffer.wrap(idArray)).array());
            char msgId = readBuffer.getChar();
            int msgLen = readBuffer.getInt();
            System.out.println("processing message header for " + id + " " + String.format("%04x", (int) msgId) + " " + msgLen);
            if (!"RC".equals(id)) {
                // invalid id... ignore
                return null;
            }
            return new Message(id, msgId, msgLen);
        }
    }


}
