# Recceiver

Recceiver is a Java/Spring Boot RecSync receiver. It accepts TCP uploads from RecCaster clients, maintains connection keepalive with ping/pong, and dispatches uploaded changes to pluggable processors via Java SPI.

Use this README in the order that fits your task:
- `Quickstart` for getting up and running quickly
- `How-To Guides` for specific setup and integration tasks
- `Reference` for exact configuration and code map details
- `Explanation` for design and runtime behavior context

## Quickstart

### 1) Prerequisites

- JDK 21 (the project compiles with Java 21)
- Maven 3.9+

### 2) Start Recceiver

From the project root, preferred option:

```shell
mvn -DskipTests package
java -jar target/recceiver-4.7.2-SNAPSHOT.jar
```

Alternative (runs directly from source via Spring Boot Maven plugin):

```shell
mvn spring-boot:run
```

The executable jar starts `org.phoebus.recceiver.RecceiverApplication`.

What to look for in logs:

- `Listening on : <bind-address>:<port>`
- `started announcing...`

By default, this is controlled by `src/main/resources/application.properties`.

### 3) Run tests (optional sanity check)

```shell
mvn test
```

---

## How-To Guides

### Configure listening address and port

Set these in `src/main/resources/application.properties`:

```ini
tcpBindAddress=0.0.0.0
tcpPort=0
```

- Use `0.0.0.0` to accept remote clients on all interfaces.
- Use `tcpPort=0` for OS-assigned ephemeral port, or set a fixed port for static deployments.

### Configure UDP discovery destination

```ini
udpBroadcastAddress=255.255.255.255
udpBroadcastPort=5049
announceInterval=15.0
```

- Keep `5049` unless your clients are configured differently.
- Increase `announceInterval` to reduce network chatter.

### Tune TCP connection behavior

```ini
tcptimeout=15.0
readBufferSize=4096
```

- `tcptimeout` controls ping cadence and timeout behavior.
- `readBufferSize` controls per-read buffer size in `MessageProcessor` (minimum 8 bytes enforced in code).

### Add a custom SPI processor

1. Implement `org.phoebus.recceiver.spi.RecSyncProcessor`.
2. Package your implementation in a JAR.
3. Add service registration file:
   - `META-INF/services/org.phoebus.recceiver.spi.RecSyncProcessor`
4. Put the JAR on Recceiver's runtime classpath.

Detailed SPI contract: `docs/spi.md`.

### Protocol and SPI docs

- RecSync protocol: `docs/recsync_protocol.md`
- SPI extension contract: `docs/spi.md`

---

## Reference

### Configuration properties

Defined in `src/main/resources/application.properties` and consumed in `src/main/java/org/phoebus/recceiver/RecceiverApplication.java`.

| Property | Default | Meaning |
|---|---:|---|
| `tcpBindAddress` | `localhost` | Interface/address used by TCP listener |
| `tcpPort` | `0` | TCP port (`0` = OS picks free port) |
| `tcptimeout` | `15.0` | Ping/pong interval and timeout window (seconds) |
| `readBufferSize` | `4096` | Per-read buffer size in bytes |
| `udpBroadcastAddress` | `255.255.255.255` | Destination address for UDP announcements |
| `udpBroadcastPort` | `5049` | Destination UDP port for announcements |
| `announceInterval` | `15.0` | UDP announce period (seconds) |
| `commitInterval` | `5.0` | Commit interval setting (currently present for future wiring) |
| `commitSizeLimit` | `0` | Commit size setting (currently present for future wiring) |

### Connection lifecycle at a glance

1. Server listens on TCP and sends periodic UDP announcements.
2. Client connects and performs handshake (`0x0001` / `0x8001`).
3. Client uploads add/delete/info messages.
4. Upload completion (`0x0005`) triggers transaction dispatch.
5. Keepalive ping/pong (`0x8002` / `0x0002`) maintains connection health.

