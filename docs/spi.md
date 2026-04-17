# RecSync SPI

Recceiver discovers processor contributions with Java `ServiceLoader`.

## SPI interfaces

- `org.phoebus.recceiver.RecSyncProcessor`
  - `commit(RecSyncTransaction transaction)` is called when client sends Upload Done (`0x0005`).
  - `connectionClosed(ConnectionClosedEvent event)` is called once when the TCP connection is lost or closed.
  - `name()` and `build(name, options)` are optional hooks on the same interface.

## Transaction payload

`RecSyncTransaction` contains:

- `sourceAddress`
- `recordsToAdd` (`List<AddRecordMessage>`)
- `recordsToDelete` (`List<DelRecordMessage>`)
- `infoMessages` (`List<AddInfoMessage>`)
- `clientInfos` (`Map<String, String>`) from client-wide Add Info messages (`recId == 0`)

## Service registration

In plugin JAR, add:

- `META-INF/services/org.phoebus.recceiver.RecSyncProcessor`

Each file should list implementation class names, one per line.

This project includes a built-in sample provider: `org.phoebus.recceiver.LogRecSyncProcessor`.
It logs transaction details on `commit(...)` and logs connection close events on `connectionClosed(...)`.


