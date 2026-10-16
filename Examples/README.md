# Android + Adonis example

Requirements: JDK 17, Android SDK 35, Node.js 24, and an Android emulator (API 24+).

```sh
npm ci --prefix Examples/Backend --no-audit --no-fund
npm start --prefix Examples/Backend
```

The real Adonis backend listens on `127.0.0.1:3333`, publishes a message every five seconds on `demo/messages`, and accepts `POST /broadcast` with `{ "text": "Hello" }`. Each message includes a globally unique id plus a server instance id and per-instance sequence, so restarting the backend cannot confuse the app's deduplication.

In another terminal:

```sh
./gradlew :example:installDebug
adb shell am start -n io.github.henriquealbert.transmit.example/.MainActivity
curl -X POST http://127.0.0.1:3333/broadcast -H 'Content-Type: application/json' -d '{"text":"Hello from the terminal"}'
```

The emulator reaches the host at `10.0.2.2:3333`. Tap **Send a message** to exercise the application's POST and see the resulting SSE message. The UI shows connection state, message count, reconnect count, and server instance identifiers.

Stop the backend with Ctrl-C, observe `Reconnecting`, then start it again. Messages should resume automatically with a new server instance id and an incremented reconnect count. For a physical device, replace the app's base URL with a reachable development host address and bind the development backend appropriately. Cleartext HTTP is enabled only in this example; use HTTPS for production applications.

The Activity owns the Transmit client and closes it in `onDestroy`; recreation creates a new connection. No credentials or environment-specific SDK paths are stored in the repository.
