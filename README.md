# transmit-kotlin

An independent Kotlin / Android client for [AdonisJS Transmit](https://docs.adonisjs.com/guides/digging-deeper/transmit), ported from [transmit-swift](https://github.com/henriquealbert/transmit-swift). Android **24+**, Kotlin coroutines, cold `Flow` consumers, OkHttp SSE, and kotlinx.serialization. MIT licensed.

## Install with JitPack

Add the repository in `settings.gradle.kts`:

```kotlin
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        maven("https://jitpack.io")
    }
}
```

Then add the Android library to your app:

```kotlin
dependencies {
    implementation("com.github.henriquealbert:transmit-kotlin:0.1.2")
}
```

Apply the Kotlin serialization plugin in your application when using `@Serializable` payloads. Maven Central publication is a future option; this release is distributed through JitPack only.

## Receive messages

```kotlin
import io.github.henriquealbert.transmit.TransmitClient
import kotlinx.coroutines.launch
import kotlinx.serialization.Serializable

@Serializable
data class Notification(val title: String, val unreadCount: Int)

val client = TransmitClient("https://api.example.com", lifecycleScope = ownerScope)
val listener = ownerScope.launch {
    try {
        client.messages<Notification>("users/42/notifications").collect { message ->
            println("${message.title}: ${message.unreadCount}")
        }
    } catch (error: Exception) {
        // Handle decoding, subscription, or terminal authentication failures.
        throw error
    }
}

listener.cancel() // Last collector on the channel triggers unsubscribe.
client.unsubscribe("users/42/notifications") // Or end every collector on this channel.
client.close() // At logout / owner disposal; permanent and idempotent.
// Optional from a coroutine: client.awaitClosed()
```

The flow is cold: collecting opens the connection and registers a consumer. Multiple collectors on the same channel share one server subscription. Cancelling a collector or using `first()` / `take()` removes it automatically. `unsubscribe(channel)` ends all current collectors on that channel. `messages(channel)` without a type returns `Flow<JsonElement>` and supports objects, arrays, scalars, and JSON `null`.

The suspending `client.awaitSubscribed(channel)` function waits for the server's successful subscribe response on the current connection. Start a collector first (or concurrently); waiting alone never subscribes or creates a hidden consumer. It returns immediately if that channel is already confirmed, and waits for a new confirmation after reconnect. Pending waits receive subscription errors, terminal authentication errors, cancellation when the last consumer leaves or the channel is unsubscribed, and a closed-client error on shutdown. Cancelling a wait does not cancel its consumers. Use an application timeout if a collector might never start; a confirmation does not prevent a later network disconnect or provide replay guarantees.

Malformed envelopes and `:` ping comments are ignored. A payload decoding failure ends only that collector; a failed subscribe request ends the channel's collectors with the HTTP error. By default, each collector has an unbounded queue. Keep processing lightweight or collect into an application-owned bounded queue when backpressure matters.

## Authentication and token refresh

```kotlin
val client = TransmitClient(
    baseUrl = "https://api.example.com",
    tokenProvider = {
        credentials.validAccessToken() // Your suspend function; refresh expired JWTs here.
    },
)

// After credentials change:
client.refreshAuthentication()
```

The suspend provider runs before every SSE GET and subscription POST. A non-null token becomes `Authorization: Bearer …`; returning `null` allows anonymous requests. To also send it in the URL, set `tokenQueryParameter = "token"`. This applies to GET and POST requests; enable query tokens only if your server requires them because URLs may be logged.

Observe `client.state: StateFlow<TransmitClient.State>` and `client.lastError: StateFlow<Throwable?>`. States are `Idle`, `Connecting`, `Connected`, `Reconnecting`, `AuthenticationFailed`, and `Closed`. HTTP errors expose `HttpStatusException.statusCode`. The latest connection error clears on a successful open. After **three consecutive 401/403 stream failures**, automatic retries stop and all consumers finish with that error. A successful open or any other connection failure resets the consecutive count. Fix credentials, call `refreshAuthentication()`, and register new collectors. A channel subscription rejection does not terminate unrelated channels.

Configure authentication and native-client CSRF rules on all three server endpoints. The provider only authenticates Transmit requests, not your application's own HTTP calls.

## Connection behavior

- `connect()` schedules opening without a channel. Wait for `state` to become `Connected` when readiness matters; this acknowledges the SSE connection, not any channel subscription.
- One UUID `uid` is generated per client and retained across retries and authentication refreshes. It is not persisted across new client instances or app launches.
- SSE uses `GET {base}/__transmit/events?uid=…`. Subscribe and unsubscribe use POST JSON `{ "uid": "…", "channel": "…" }`, only while the stream is open.
- Reconnect delays are **1, 2, 4, 8, 16, 30 seconds**, capped at 30. A successful stream open resets the delay, including quiet streams with no data events. Reopening restores every channel that still has collectors. Explicit credential refresh cancels the current stream or retry delay and reopens immediately with fresh credentials, without waiting for backoff.
- Transmit does not replay messages lost while disconnected. Backends implementing replay or redelivery may produce **duplicate events after resubscribe**. Include a stable `seq` and deduplicate within its channel scope; never reuse that sequence across backend restarts in the same scope.
- Base URLs may have a path prefix but must not contain credentials, query parameters, or fragments. Use HTTPS in production.
- `close()` cancels the owned coroutine scope, SSE call, retry delay, and pending POSTs, and ends consumers. Closing the SSE connection lets Adonis remove that client's server subscriptions. It does not wait for cleanup POSTs while offline. `awaitClosed()` waits for cancellation to settle; create a new client to start again.
- A supplied `OkHttpClient` stays under the caller's ownership. The default client and its connection pool/executor are released on close. For leak protection, pass an owner scope such as an Activity's `lifecycleScope` or a ViewModel's `viewModelScope`: `TransmitClient(baseUrl, lifecycleScope = ownerScope)`. Cancellation of that scope closes the owned child scope, connection, pending requests, consumers, and confirmation waits even if `close()` was forgotten; closing the client never cancels the owner scope. This works on Android 24+ and preserves the original constructor. Without lifecycle binding, explicit `close()` remains required; garbage collection is not a cleanup guarantee.

## Read timeout

The default client uses a **90-second idle/read timeout**. Set your server's ping interval comfortably below it. Pings keep the connection alive even when channels are quiet. For different networking settings, inject an OkHttp client:

```kotlin
val http = okhttp3.OkHttpClient.Builder()
    .readTimeout(90, java.util.concurrent.TimeUnit.SECONDS)
    .build()
val client = TransmitClient("https://api.example.com", client = http)
```

The read timeout measures waiting for data, not the total connection lifetime. OkHttp SSE disables the overall call timeout after opening the stream. Custom headers, cookies, interceptors, TLS configuration, and POST timeouts belong on your supplied client. You own its eventual shutdown.

## Chat example: subscribe, publish, and clean up

A neutral chat channel such as `chats/general` carries `{ "seq": "…", "sender": "…", "text": "…", "sentAt": "…" }`. A separate channel such as `users/42/notifications` can carry notifications; apply your application's authorization to user-specific channels. Channel names and payload shapes are application-defined, and the client supports both without additional configuration.

This short Adonis example accepts `POST /messages` and broadcasts to the chat channel. Register the Transmit routes only once in your application:

```typescript
import { randomUUID } from 'node:crypto'
import router from '@adonisjs/core/services/router'
import transmit from '@adonisjs/transmit/services/main'

transmit.registerRoutes()
router.post('/messages', ({ request, response }) => {
  const text = request.input('text')
  if (typeof text !== 'string' || !text.trim()) {
    return response.badRequest({ error: 'A message must contain text.' })
  }
  const message = {
    seq: randomUUID(), // Stable event identity; retain it if you redeliver this message.
    sender: 'guest',
    text: text.trim(),
    sentAt: new Date().toISOString(),
  }
  transmit.broadcast('chats/general', message)
  return message
})
```

Start the Kotlin collector before awaiting confirmation. `awaitSubscribed(channel)` completes after the subscribe POST succeeds, so the following publish request can reach this consumer. It does not create a consumer itself or guarantee delivery after a later disconnect.

```kotlin
import io.github.henriquealbert.transmit.TransmitClient
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody

@Serializable
data class ChatMessage(val seq: String, val sender: String, val text: String, val sentAt: String)

suspend fun sendAndReceive(baseUrl: String, http: OkHttpClient): ChatMessage =
    withTimeout(30_000) {
        val client = TransmitClient(baseUrl, lifecycleScope = this)
        val channel = "chats/general"
        val seen = mutableSetOf<String>()
        val received = CompletableDeferred<ChatMessage>()
        val listener = launch(start = CoroutineStart.UNDISPATCHED) {
            client.messages<ChatMessage>(channel)
                .filter { seen.add(it.seq) }
                .collect { message ->
                    println("${message.sender}: ${message.text}")
                    received.complete(message) // This demonstration waits for one message.
                }
        }
        try {
            client.awaitSubscribed(channel)
            withContext(Dispatchers.IO) {
                val body = buildJsonObject { put("text", "Hello, everyone!") }.toString()
                val request = Request.Builder().url("${baseUrl.trimEnd('/')}/messages")
                    .post(body.toRequestBody("application/json".toMediaType())).build()
                http.newCall(request).execute().use { response ->
                    check(response.isSuccessful) { "Publish rejected: ${response.code}" }
                }
            }
            received.await()
        } finally {
            client.unsubscribe(channel)
            listener.cancel()
            client.close()
            withContext(NonCancellable) { listener.join(); client.awaitClosed() }
        }
    }
```

On a shared channel, the first received message may be from another sender. In a screen or service, keep collecting for that owner's lifetime, then cancel its collector or call `unsubscribe(channel)` and close the client when the owner is finished. Configure authentication on your `http` client for application POSTs as well as on the Transmit client for its own endpoints.

Keep an event's `seq` stable when redelivering it and deduplicate per channel. The runnable [Android and Adonis example](Examples/README.md) uses a server-instance identifier plus a sequence counter so a restart cannot reuse an earlier event identity. Transmit reconnects and restores subscriptions automatically, but does not replay messages missed offline; persist and replay messages in your backend if the application needs that guarantee.

## Build, tests, and coverage

Install JDK 17, Android SDK platform/build-tools 35, and Node.js 24. Set `JAVA_HOME` and `ANDROID_HOME`, then:

```sh
npm ci --prefix integration-server --no-audit --no-fund
npm ci --prefix Examples/Backend --no-audit --no-fund
./gradlew assembleRelease :core:test :core:integrationTest :core:koverVerify :core:koverXmlReport :example:assembleDebug
```

The JVM `:core` module compiles the **same source files** as the Android AAR. Unit tests cover requests, parsing, sharing, decoding errors, backoff reset on opening, immediate credential refresh, subscribe acknowledgement and cancellation, lifecycle-owned cleanup, auth exhaustion, and HTTP failures. Integration tests launch a real loopback AdonisJS server and validate actual routing, auth, subscriptions, broadcasts, socket loss, malformed MIME type, EOF, and token rotation. A second integration test launches the example backend and verifies confirmed chat publishing, unsubscribe, and a fresh subscription. Integration retry waits are shortened; unit tests separately verify the production delay sequence.

CI runs the full validation suite against both tested combinations:

| AdonisJS | `@adonisjs/transmit` | Integration fixture |
| --- | --- | --- |
| 7.6.0 | 3.0.2 | Default, installed from `integration-server/package-lock.json` |
| 6.21.0 | 2.0.2 | Compatibility matrix entry; also used by the example backend |

Transmit 3.0.2 declares the AdonisJS peer dependency `^7.0.0-next`, which accepts stable v7. Both combinations pass with the same Kotlin client; no protocol changes are required. The shared test server configures the separate encryption service required by v7 and retains v6's `app.appKey`. To run the v6 fixture locally after the default installation:

```sh
npm install --prefix integration-server --no-save --package-lock=false --no-audit --no-fund @adonisjs/core@6.21.0 @adonisjs/transmit@2.0.2
./gradlew :core:integrationTest :core:koverVerify :core:koverXmlReport
```

The v6 override leaves the committed v7 manifest and lockfile intact; its transitive dependencies resolve at install time. Run `npm ci --prefix integration-server --no-audit --no-fund` to restore the locked v7 fixture. Gradle tracks the installed server versions so switching between fixtures reruns the integration tests.

Kover enforces **100% library line coverage** across all production Kotlin sources, with no source exclusions. A missing line fails `:core:koverVerify`; the gate aggregates unit and integration tests. Branch/instruction counts are also available in `core/build/reports/kover/report.xml`. Example UI and the JavaScript fixtures are outside the library coverage metric. CI builds the AAR and Compose example, runs both suites, verifies coverage, and uploads test/coverage reports and the example APK.

See [Examples](Examples/README.md) for running the Compose app and its Adonis backend.

## License

MIT. This is a community client, not an official AdonisJS package.
