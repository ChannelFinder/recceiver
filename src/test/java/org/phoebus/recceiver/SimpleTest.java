package org.phoebus.recceiver;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

public class SimpleTest {

    @Test
    public void encoding() {
        String hexString = "1234567890";
        byte[] bytes = HexFormat.of().parseHex(hexString);

        System.out.println(HexFormat.of().formatHex(bytes));

        ByteBuffer readBuffer = ByteBuffer.wrap(bytes);
        ByteBuffer dstBuffer = ByteBuffer.allocate(16);

        System.out.println("dst remaining: " + dstBuffer.remaining() + " dst position: " + dstBuffer.position());
        System.out.println("read remaining: " + readBuffer.remaining() + " read position: " + readBuffer.position());

        dstBuffer.put(readBuffer.array(), 0, 2);
        readBuffer.position(readBuffer.position() + 2);
        System.out.println(HexFormat.of().formatHex(dstBuffer.array()));

        System.out.println("dst remaining: " + dstBuffer.remaining() + " dst position: " + dstBuffer.position());
        System.out.println("read remaining: " + readBuffer.remaining() + " read position: " + readBuffer.position());

        dstBuffer.put(readBuffer.array(), readBuffer.position(), 1);
        readBuffer.position(readBuffer.position() + 1);
        System.out.println(HexFormat.of().formatHex(dstBuffer.array()));
        dstBuffer.put(readBuffer.array(), readBuffer.position(), 1);
        readBuffer.position(readBuffer.position() + 1);
        System.out.println(HexFormat.of().formatHex(dstBuffer.array()));

        System.out.println("dst remaining: " + dstBuffer.remaining() + " dst position: " + dstBuffer.position());
        System.out.println("read remaining: " + readBuffer.remaining() + " read position: " + readBuffer.position());

        //dstBuffer.position(0);
        dstBuffer.flip();
        System.out.println(StandardCharsets.UTF_8.decode(dstBuffer));
        readBuffer.position(0);
        System.out.println(StandardCharsets.UTF_8.decode(readBuffer));
    }

    @Test
    public void compact() {

        String hexString = "1234567890";
        byte[] bytes = HexFormat.of().parseHex(hexString);
        ByteBuffer readBuffer = ByteBuffer.allocate(5);
        readBuffer.put(bytes, 0, 5);

        System.out.println(HexFormat.of().formatHex(readBuffer.array()) + " " + readBuffer.position() + " " + readBuffer.limit());
        readBuffer.flip();
        System.out.println(HexFormat.of().formatHex(readBuffer.array()) + " " + readBuffer.position() + " " + readBuffer.limit());
        readBuffer.get();
        readBuffer.get();
        readBuffer.get();
        readBuffer.get();
        System.out.println(HexFormat.of().formatHex(readBuffer.array()) + " " + readBuffer.position() + " " + readBuffer.limit());

        readBuffer.compact();
        System.out.println(HexFormat.of().formatHex(readBuffer.array()) + " " + readBuffer.position() + " " + readBuffer.limit());

        readBuffer.put((byte) 0xFF);
        readBuffer.put((byte) 0xFF);
        readBuffer.put((byte) 0xFF);
        readBuffer.put((byte) 0xFF);
        System.out.println(HexFormat.of().formatHex(readBuffer.array()) + " " + readBuffer.position() + " " + readBuffer.limit());


    }
}
