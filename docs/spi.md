# RecSync SPI

Recceiver discovers processor contributions with Java `ServiceLoader`.

## SPI interfaces

All SPI types live in `org.phoebus.recceiver.spi`.

- `RecSyncProcessor`
  - `init(String name, Map<String, String> options)` is the optional setup hook for a processor instance.
  - `commit(RecSyncTransaction transaction)` is called when client sends Upload Done (`0x0005`).
  - `connectionClosed(ConnectionClosedEvent event)` is called once when the TCP connection is lost or closed.
  - `destroy()` is the optional teardown hook that complements `init(...)`.
  - `name()` is an optional hook on the same interface.

## Transaction payload

`RecSyncTransaction` contains:

- `sourceAddress`
- `recordsToAdd` (`List<AddRecordMessage>`)
- `recordsToDelete` (`List<DelRecordMessage>`)
- `infoMessages` (`List<AddInfoMessage>`)
- `clientInfos` (`Map<String, String>`) from client-wide Add Info messages (`recId == 0`)

## Service registration

In plugin JAR, add:

- `META-INF/services/org.phoebus.recceiver.spi.RecSyncProcessor`

Each file should list implementation class names, one per line.

This project includes a built-in sample provider: `org.phoebus.recceiver.spi.log.LogRecSyncProcessor`.
It logs transaction details on `commit(...)` and logs connection close events on `connectionClosed(...)`.

Recceiver discovers processor contributions with Java `ServiceLoader`.

## SPI interfaces

- `org.phoebus.recceiver.spi.RecSyncProcessor`
  - `init(String name, Map<String, String> options)` is the optional setup hook for a processor instance.
  - `commit(RecSyncTransaction transaction)` is called when client sends Upload Done (`0x0005`).
  - `connectionClosed(ConnectionClosedEvent event)` is called once when the TCP connection is lost or closed.
  - `destroy()` is the optional teardown hook that complements `init(...)`.
  - `name()` is an optional hook on the same interface.

## Transaction payload

`RecSyncTransaction` contains:

- `sourceAddress`
- `recordsToAdd` (`List<AddRecordMessage>`)
- `recordsToDelete` (`List<DelRecordMessage>`)
- `infoMessages` (`List<AddInfoMessage>`)
- `clientInfos` (`Map<String, String>`) from client-wide Add Info messages (`recId == 0`)

## Service registration

In plugin JAR, add:

- `META-INF/services/org.phoebus.recceiver.spi.RecSyncProcessor`

Each file should list implementation class names, one per line.

This project includes a built-in sample provider: `org.phoebus.recceiver.spi.log.LogRecSyncProcessor`.
It logs transaction details on `commit(...)` and logs connection close events on `connectionClosed(...)`.


