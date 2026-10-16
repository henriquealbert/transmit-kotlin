package io.github.henriquealbert.transmit

import java.io.IOException
import java.util.UUID
import kotlin.test.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.test.*
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.*
import okhttp3.*
import okhttp3.sse.*
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class ClientTest {
    class FakeSource(private val request: Request) : EventSource {
        var cancelled = false
        override fun request() = request
        override fun cancel() { cancelled = true }
    }
    class FakeTransport : Transport {
        val sources = mutableListOf<FakeSource>()
        val listeners = mutableListOf<EventSourceListener>()
        val posts = mutableListOf<Request>()
        var post: suspend (Request) -> Unit = { }
        var closed = false
        override fun open(request: Request, listener: EventSourceListener): EventSource = FakeSource(request).also {
            sources += it; listeners += listener
        }
        override suspend fun send(request: Request) { posts += request; post(request) }
        override fun close() { closed = true }
        fun opened() { listeners.last().onOpen(sources.last(), response()) }
        fun event(data: String) { listeners.last().onEvent(sources.last(), null, null, data) }
        fun end() { listeners.last().onClosed(sources.last()) }
        fun fail(code: Int? = null, error: Throwable? = null) {
            listeners.last().onFailure(sources.last(), error, code?.let { response(it) })
        }
        fun response(code: Int = 200) = Response.Builder().request(sources.last().request()).protocol(Protocol.HTTP_1_1).code(code).message("test").build()
    }
    class Fixture(val scope: TestScope, token: suspend () -> String? = { null }) {
        val transport = FakeTransport()
        val waits = mutableListOf<Long>()
        val client = TransmitClient(Requests("https://example.com", null, token), transport, StandardTestDispatcher(scope.testScheduler)) {
            waits += it; delay(it)
        }
        fun start() { client.connect(); scope.runCurrent(); transport.opened(); scope.runCurrent() }
        suspend fun close() { client.close(); scope.runCurrent(); client.awaitClosed(); assertTrue(transport.closed) }
        fun retry() { scope.advanceTimeBy(waits.last()); scope.runCurrent() }
    }

    @Test fun `URL requests preserve prefix and credentials safely`() = runTest {
        val requests = Requests("https://example.com/api/", "access_token") { "a +&b" }
        val stream = requests.create("events", "user")
        assertEquals("/api/__transmit/events", stream.url.encodedPath)
        assertEquals("user", stream.url.queryParameter("uid"))
        assertEquals("a +&b", stream.url.queryParameter("access_token"))
        assertEquals("Bearer a +&b", stream.header("Authorization"))
        val post = requests.create("subscribe", "user", "a/b")
        val buffer = okio.Buffer(); post.body!!.writeTo(buffer)
        assertEquals(buildJsonObject { put("uid", "user"); put("channel", "a/b") }, Json.parseToJsonElement(buffer.readUtf8()))
        assertNull(post.url.queryParameter("uid"))
        assertEquals("application/json; charset=utf-8", post.body!!.contentType().toString())
        val anonymous = Requests("http://localhost/api", "token") { null }.create("events", "user")
        assertNull(anonymous.header("Authorization"))
        assertNull(anonymous.url.queryParameter("token"))
        for (url in listOf("ftp://example.com", "https://u:p@example.com", "https://example.com?q=x", "https://example.com/#a")) {
            assertFailsWith<IllegalArgumentException> { Requests(url, null) { null } }
        }
    }

    @Test fun `envelopes ignore malformed data and accept every JSON payload`() {
        for (data in listOf("", "no", "[]", "null", "{}", "{\"channel\":5,\"payload\":1}", "{\"channel\":\"a\"}", "{\"channel\":{},\"payload\":1}")) assertNull(parseEnvelope(data))
        for (payload in listOf("null", "true", "1", "\"text\"", "[]", "{}")) {
            assertEquals("a" to Json.parseToJsonElement(payload), parseEnvelope("{\"channel\":\"a\",\"payload\":$payload}"))
        }
    }

    @Test fun `collectors share subscribe after open and last cancellation unsubscribes`() = runTest {
        val f = Fixture(this)
        val a = mutableListOf<Int>(); val b = mutableListOf<JsonElement>()
        val one = backgroundScope.launch { f.client.messages<Int>("room").collect { a += it } }
        val two = backgroundScope.launch { f.client.messages("room").collect { b += it } }
        runCurrent()
        assertTrue(f.transport.posts.isEmpty())
        assertEquals(TransmitClient.State.Connecting, f.client.state.value)
        assertEquals(f.client.uid, UUID.fromString(f.client.uid).toString())
        f.client.connect(); runCurrent()
        assertEquals(1, f.transport.sources.size)
        f.transport.opened(); runCurrent()
        assertEquals(1, f.transport.posts.size)
        f.transport.event("bad"); f.transport.event("{\"channel\":\"room\",\"payload\":42}"); runCurrent()
        assertEquals(listOf(42), a); assertEquals(listOf<JsonElement>(JsonPrimitive(42)), b)
        one.cancel(); runCurrent(); assertEquals(1, f.transport.posts.size)
        two.cancel(); runCurrent(); assertEquals("/__transmit/unsubscribe", f.transport.posts.last().url.encodedPath)
        f.client.unsubscribe("missing"); runCurrent()
        f.transport.event("{\"channel\":\"missing\",\"payload\":null}"); runCurrent()
        f.close()
        assertTrue(f.transport.sources.single().cancelled)
        f.client.close(); f.client.connect(); f.client.refreshAuthentication(); f.client.unsubscribe("room")
        assertFailsWith<ClientClosedException> { f.client.messages("room").first() }
    }

    @Test fun `connected addition explicit unsubscribe decoding failure and failed post`() = runTest {
        val f = Fixture(this); f.start()
        val first = backgroundScope.launch { f.client.messages("a").collect() }; runCurrent()
        val second = backgroundScope.launch { f.client.messages("a").collect() }; runCurrent()
        assertEquals(1, f.transport.posts.size)
        f.client.unsubscribe("a"); runCurrent()
        assertTrue(first.isCompleted); assertTrue(second.isCompleted)
        var decodeError: Throwable? = null
        backgroundScope.launch { try { f.client.messages<Int>("b").collect() } catch (e: Throwable) { decodeError = e } }
        runCurrent(); f.transport.event("{\"channel\":\"b\",\"payload\":\"not an integer\"}"); runCurrent()
        assertIs<SerializationException>(decodeError)
        f.transport.post = { throw IOException("offline") }
        var subscriptionError: Throwable? = null
        backgroundScope.launch { try { f.client.messages("bad").collect() } catch (e: Throwable) { subscriptionError = e } }
        runCurrent(); assertEquals("offline", subscriptionError?.message)
        f.transport.post = { }
        val last = backgroundScope.launch { f.client.messages("last").collect() }; runCurrent()
        f.transport.post = { throw IOException("unsubscribe failed") }
        last.cancel(); runCurrent()
        assertEquals(TransmitClient.State.Connected, f.client.state.value)
        f.close()
    }

    @Test fun `reconnect backoff caps restores channels and ignores stale callbacks`() = runTest {
        val f = Fixture(this)
        backgroundScope.launch { f.client.messages("room").collect() }; runCurrent()
        f.transport.opened(); runCurrent()
        val originalListener = f.transport.listeners.first(); val originalSource = f.transport.sources.first()
        repeat(7) { f.transport.end(); runCurrent(); f.retry(); f.transport.opened(); runCurrent() }
        assertEquals(listOf(1000L, 2000L, 4000L, 8000L, 16000L, 30000L, 30000L), f.waits)
        assertEquals(8, f.transport.posts.size)
        assertEquals(1, f.transport.sources.map { it.request().url.queryParameter("uid") }.toSet().size)
        originalListener.onClosed(originalSource); originalListener.onEvent(originalSource, null, null, "{}"); runCurrent()
        assertEquals(TransmitClient.State.Connected, f.client.state.value)
        f.transport.event("{\"channel\":\"room\",\"payload\":1}"); runCurrent()
        f.client.refreshAuthentication(); runCurrent()
        assertEquals(1000L, f.waits.last())
        f.client.connect(); runCurrent(); assertEquals(8, f.transport.sources.size)
        f.client.unsubscribe("room"); runCurrent()
        f.close()
    }

    @Test fun `three consecutive auth failures stop and explicit refresh recovers`() = runTest {
        var token = "first"
        val f = Fixture(this) { token }
        var failure: Throwable? = null
        backgroundScope.launch { try { f.client.messages("a").collect() } catch (e: Throwable) { failure = e } }; runCurrent()
        repeat(3) { index -> f.transport.fail(if (index == 1) 403 else 401); runCurrent(); if (index < 2) f.retry() }
        assertEquals(TransmitClient.State.AuthenticationFailed, f.client.state.value)
        assertEquals(401, (failure as HttpStatusException).statusCode)
        assertSame(failure, f.client.lastError.value)
        val rejected = async { runCatching { f.client.messages("new").first() } }
        runCurrent(); assertIs<HttpStatusException>(rejected.await().exceptionOrNull())
        f.client.connect(); runCurrent(); assertEquals(3, f.transport.sources.size)
        token = "fresh"; f.client.refreshAuthentication(); runCurrent()
        assertEquals("Bearer fresh", f.transport.sources.last().request().header("Authorization"))
        assertNull(f.client.lastError.value)
        f.transport.opened(); runCurrent()
        assertEquals(TransmitClient.State.Connected, f.client.state.value)
        f.close()
    }

    @Test fun `non auth errors and successful open reset auth count`() = runTest {
        val f = Fixture(this); f.client.connect(); runCurrent()
        f.transport.fail(401); runCurrent(); f.retry()
        f.transport.fail(500); runCurrent(); f.retry()
        repeat(2) { f.transport.fail(403); runCurrent(); f.retry() }
        f.transport.opened(); runCurrent()
        f.transport.fail(401); runCurrent(); f.retry()
        f.transport.fail(error = IOException("socket")); runCurrent(); assertEquals("socket", f.client.lastError.value?.message); f.retry()
        f.transport.fail(); runCurrent(); assertEquals("SSE stream failed", f.client.lastError.value?.message)
        f.close()
    }

    @Test fun `provider errors retry and close cancels a suspended provider`() = runTest {
        var suspended = false
        val f = Fixture(this) { if (!suspended) throw IOException("token") else awaitCancellation() }
        f.client.connect(); runCurrent(); assertEquals("token", f.client.lastError.value?.message)
        suspended = true; f.retry(); f.close()
        assertEquals(TransmitClient.State.Closed, f.client.state.value)
    }

    @Test fun `close cancels subscribe and unsubscribe and pending collectors`() = runTest {
        for (action in listOf("subscribe", "unsubscribe")) {
            val f = Fixture(this); f.start()
            f.transport.post = { if (it.url.encodedPath.endsWith(action)) awaitCancellation() }
            val consumer = backgroundScope.launch { f.client.messages("a").collect() }; runCurrent()
            if (action == "unsubscribe") { consumer.cancel(); runCurrent() }
            f.close(); runCurrent(); assertTrue(consumer.isCompleted)
        }
        val f = Fixture(this)
        val consumer = backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) { f.client.messages("pending").collect() }
        f.close(); runCurrent(); assertTrue(consumer.isCompleted)
    }
}
