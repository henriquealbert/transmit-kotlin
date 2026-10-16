package io.github.henriquealbert.transmit

import java.io.File
import java.util.concurrent.TimeUnit
import kotlin.test.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.serialization.json.*
import okhttp3.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import org.junit.Test

/** Launches actual Adonis providers, router, body parser, and Transmit controllers. */
class IntegrationTest {
    @Test fun `real Adonis SSE auth subscriptions broadcasts reconnect and cleanup`() = runBlocking {
        val script = File(System.getProperty("transmit.server"))
        val server = ProcessBuilder("node", script.absolutePath).directory(script.parentFile).redirectError(ProcessBuilder.Redirect.INHERIT).start()
        val http = OkHttpClient.Builder().readTimeout(5, TimeUnit.SECONDS).build()
        var client: TransmitClient? = null
        try {
            val base = withContext(Dispatchers.IO) { server.inputStream.bufferedReader().readLine() }
            assertTrue(base.startsWith("http://127.0.0.1:"), base)
            fun get(path: String): JsonElement = http.newCall(Request.Builder().url(base + path).build()).execute().use { Json.parseToJsonElement(it.body!!.string()) }
            fun post(path: String, body: String) {
                http.newCall(Request.Builder().url(base + path).post(body.toRequestBody("application/json".toMediaType())).build()).execute().use { assertTrue(it.isSuccessful, "${it.code}: $path") }
            }
            suspend fun eventually(condition: () -> Boolean) { withTimeout(20_000) { while (!condition()) delay(25) } }
            fun history() = get("/history").jsonArray
            fun count(action: String, channel: String? = null) = history().count {
                it.jsonObject["path"]!!.jsonPrimitive.content.endsWith(action) &&
                    (channel == null || it.jsonObject["body"]?.jsonObject?.get("channel")?.jsonPrimitive?.content == channel)
            }
            var token = "first"
            val transmit = TransmitClient(Requests(base, "token") { token }, OkHttpTransport(null), Dispatchers.IO) { delay(50) }
            client = transmit
            val received = mutableListOf<JsonElement>()
            val collector = launch { transmit.messages("room").collect { received += it } }
            eventually { count("subscribe", "room") == 1 }
            val records = history()
            assertTrue(records.first().jsonObject["path"]!!.jsonPrimitive.content.endsWith("events"))
            assertTrue(records.all { it.jsonObject["authorization"]!!.jsonPrimitive.content == "Bearer first" })
            assertEquals(transmit.uid, records.first().jsonObject["query"]!!.jsonObject["uid"]!!.jsonPrimitive.content)
            assertEquals("first", records.first().jsonObject["query"]!!.jsonObject["token"]!!.jsonPrimitive.content)
            post("/broadcast", """{"channel":"room","payload":{"seq":1,"text":"hello"}}""")
            eventually { received.size == 1 }
            assertEquals("hello", received.single().jsonObject["text"]!!.jsonPrimitive.content)
            post("/control", """{"disconnect":true}""")
            eventually { count("subscribe", "room") >= 2 }
            post("/broadcast", """{"channel":"room","payload":{"seq":2}}""")
            eventually { received.size == 2 }; assertEquals(2, received.last().jsonObject["seq"]!!.jsonPrimitive.int)
            token = "second"; post("/control", """{"token":"second"}""")
            transmit.refreshAuthentication()
            eventually { count("subscribe", "room") >= 3 }
            assertTrue(history().any { it.jsonObject["authorization"]!!.jsonPrimitive.content == "Bearer second" })
            val denied = async { runCatching { transmit.messages("denied").first() } }
            val error = withTimeout(5000) { denied.await().exceptionOrNull() }
            assertEquals(400, (error as HttpStatusException).statusCode)
            collector.cancelAndJoin()
            eventually { count("unsubscribe", "room") == 1 }
            // Invalid MIME type and graceful EOF must both cause a fresh connection.
            val eventsBefore = count("events")
            post("/control", """{"invalidContent":true,"disconnect":true}""")
            eventually { count("events") >= eventsBefore + 2 && transmit.state.value == TransmitClient.State.Connected }
            post("/control", """{"endNext":true,"disconnect":true}""")
            eventually { count("events") >= eventsBefore + 4 && transmit.state.value == TransmitClient.State.Connected }
            // Repeated unauthorized GETs stop; repairing the provider explicitly resumes.
            token = "expired"; transmit.refreshAuthentication()
            withTimeout(20_000) { transmit.state.first { it == TransmitClient.State.AuthenticationFailed } }
            assertEquals(401, (transmit.lastError.value as HttpStatusException).statusCode)
            token = "second"; transmit.refreshAuthentication()
            withTimeout(10_000) { transmit.state.first { it == TransmitClient.State.Connected } }
            transmit.close(); transmit.awaitClosed()
            assertEquals(TransmitClient.State.Closed, transmit.state.value)
        } finally {
            client?.close(); client?.awaitClosed()
            http.dispatcher.executorService.shutdown(); http.connectionPool.evictAll()
            server.destroy()
            if (!server.waitFor(5, TimeUnit.SECONDS)) { server.destroyForcibly(); server.waitFor() }
        }
    }
}
