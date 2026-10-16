# Android + Adonis example

Requirements: JDK 17, Android SDK 35, Node.js 24, and an Android emulator (API 24+).

```sh
npm ci --prefix Examples/Backend --no-audit --no-fund
npm start --prefix Examples/Backend
```

The real Adonis backend listens on `127.0.0.1:3333`, publishes a message every five seconds on `chats/general`, and accepts `POST /messages` with `{ "text": "Hello" }`. Each message contains `seq`, `sender`, `text`, and `sentAt`. Its `seq` combines a server-instance UUID and a counter, so restarting the backend cannot reuse an earlier event identity.

In another terminal:

```sh
./gradlew :example:installDebug
adb shell am start -n io.github.henriquealbert.transmit.example/.MainActivity
curl -X POST http://127.0.0.1:3333/messages -H 'Content-Type: application/json' -d '{"text":"Hello from the terminal"}'
```

The emulator reaches the host at `10.0.2.2:3333`. The app subscribes to `chats/general` on launch. Tap **Send a message** to wait for `awaitSubscribed(channel)`, publish via POST, and receive the SSE message. Use **Unsubscribe** to stop receiving and **Subscribe** to resume; new events are deduplicated by `seq`. The UI shows connection state, message count, reconnect count, and shortened server-instance identifiers.

Stop the backend with Ctrl-C, observe `Reconnecting`, then start it again. Messages should resume automatically with a new server instance id and an incremented reconnect count. For a physical device, replace the app's base URL with a reachable development host address and bind the development backend appropriately. Cleartext HTTP is enabled only in this example; use HTTPS for production applications.

The Activity binds the Transmit client to its `lifecycleScope` and also closes it in `onDestroy`; cancelling that scope guarantees cleanup even if the explicit close is forgotten, and recreation creates a new connection. No credentials or environment-specific SDK paths are stored in the repository.
