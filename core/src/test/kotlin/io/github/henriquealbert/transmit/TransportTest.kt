package io.github.henriquealbert.transmit

import java.io.IOException
import java.util.concurrent.TimeUnit
import kotlin.test.*
import kotlinx.coroutines.*
import okhttp3.*
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.mockwebserver.*
import okhttp3.sse.EventSource
import okhttp3.sse.EventSourceListener
import org.junit.Test

class TransportTest {
    @Test fun `events cookie is returned on subscribe and unsubscribe with default client`() = runBlocking {
        assertStickyCookie(OkHttpTransport(null))
    }

    @Test fun `NO_COOKIES caller gets a private jar while sharing caller resources`() = runBlocking {
        val client = OkHttpClient()
        val transport = OkHttpTransport(client)
        val another = OkHttpTransport(client)
        try {
            assertNotSame(client, transport.client)
            assertSame(CookieJar.NO_COOKIES, client.cookieJar)
            assertNotSame(transport.client.cookieJar, another.client.cookieJar)
            assertSame(client.dispatcher, transport.client.dispatcher)
            assertSame(client.connectionPool, transport.client.connectionPool)
            assertStickyCookie(transport)
            assertFalse(client.dispatcher.executorService.isShutdown)
        } finally {
            another.close()
            client.dispatcher.executorService.shutdown(); client.connectionPool.evictAll()
        }
    }

    @Test fun `caller custom jar is respected for stream and subscription requests`() = runBlocking {
        val jar = InMemoryCookieJar()
        val client = OkHttpClient.Builder().cookieJar(jar).build()
        val transport = OkHttpTransport(client)
        try {
            assertSame(client, transport.client)
            assertSame(jar, transport.client.cookieJar)
            assertStickyCookie(transport)
            assertFalse(client.dispatcher.executorService.isShutdown)
        } finally {
            client.dispatcher.executorService.shutdown(); client.connectionPool.evictAll()
        }
    }

    private suspend fun assertStickyCookie(transport: OkHttpTransport) {
        val server = MockWebServer(); server.start()
        var stream: EventSource? = null
        try {
            server.enqueue(MockResponse().setHeader("Content-Type", "text/event-stream")
                .addHeader("Set-Cookie", "sticky=pod-a; Path=/__transmit; HttpOnly")
                .addHeader("Set-Cookie", "expired=gone; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT")
                .setBody(": ping\n\n"))
            val requests = Requests(server.url("/").toString(), null) { null }
            val opened = CompletableDeferred<Unit>()
            stream = transport.open(requests.create("events", "stream"), object : EventSourceListener() {
                override fun onOpen(eventSource: EventSource, response: Response) { opened.complete(Unit) }
                override fun onFailure(eventSource: EventSource, t: Throwable?, response: Response?) {
                    opened.completeExceptionally(t ?: IOException("SSE failed"))
                }
            })
            withTimeout(5000) { opened.await() }
            assertNull(server.takeRequest(5, TimeUnit.SECONDS)!!.getHeader("Cookie"))
            for (action in listOf("subscribe", "unsubscribe")) {
                server.enqueue(MockResponse().setResponseCode(204))
                transport.send(requests.create(action, "stream", "chat"))
                val post = server.takeRequest(5, TimeUnit.SECONDS)!!
                assertEquals("POST", post.method)
                assertEquals("/__transmit/$action", post.path)
                assertEquals("sticky=pod-a", post.getHeader("Cookie"))
            }
        } finally { stream?.cancel(); transport.close(); server.shutdown() }
    }

    @Test fun `jar matches domain path and secure and replaces cookies by name domain path`() {
        val jar = InMemoryCookieJar()
        val url = "https://api.example.com/__transmit/events".toHttpUrl()
        jar.saveFromResponse(url, listOf(
            Cookie.parse(url, "host=one; Path=/")!!,
            Cookie.parse(url, "domain=shared; Domain=example.com; Path=/")!!,
            Cookie.parse(url, "sticky=old; Path=/__transmit; Secure")!!,
            Cookie.parse(url, "sticky=root; Path=/")!!,
        ))
        jar.saveFromResponse(url, listOf(Cookie.parse(url, "sticky=new; Path=/__transmit; Secure")!!))
        assertEquals(listOf("sticky=new", "host=one", "domain=shared", "sticky=root"),
            jar.loadForRequest(url).map { "${it.name}=${it.value}" })
        assertEquals(listOf("domain"), jar.loadForRequest("https://other.example.com/".toHttpUrl()).map { it.name })
        assertTrue(jar.loadForRequest("https://unrelated.test/".toHttpUrl()).isEmpty())
        for (other in listOf("http://api.example.com/__transmit/events", "https://api.example.com/__transmit-extra")) {
            assertEquals(listOf("host", "domain", "sticky"), jar.loadForRequest(other.toHttpUrl()).map { it.name })
        }
    }

    @Test fun `expired cookies delete prior values and expire between requests`() = runBlocking {
        val jar = InMemoryCookieJar()
        val url = "https://example.com/".toHttpUrl()
        jar.saveFromResponse(url, listOf(Cookie.parse(url, "sticky=live; Path=/")!!))
        assertEquals("live", jar.loadForRequest(url).single().value)
        jar.saveFromResponse(url, listOf(Cookie.parse(url, "sticky=deleted; Path=/; Max-Age=0")!!))
        assertTrue(jar.loadForRequest(url).isEmpty())
        jar.saveFromResponse(url, listOf(Cookie.Builder().name("short").value("lived")
            .hostOnlyDomain(url.host).expiresAt(System.currentTimeMillis() + 100).build()))
        delay(150)
        assertTrue(jar.loadForRequest(url).isEmpty())
    }

    @Test fun `parallel cookie saves and loads retain distinct cookies`() = runBlocking {
        val jar = InMemoryCookieJar()
        val url = "https://example.com/".toHttpUrl()
        coroutineScope {
            repeat(64) { index -> launch(Dispatchers.Default) {
                jar.saveFromResponse(url, listOf(Cookie.parse(url, "cookie$index=value; Path=/")!!))
                assertTrue(jar.loadForRequest(url).isNotEmpty())
            } }
        }
        assertEquals(64, jar.loadForRequest(url).size)
    }

    @Test fun `real HTTP POST supports success status failure network failure and cancellation`() = runBlocking {
        val server = MockWebServer(); server.start()
        val transport = OkHttpTransport(null)
        try {
            assertEquals(90_000, transport.client.readTimeoutMillis)
            val request = Request.Builder().url(server.url("/post")).header("Connection", "close").build()
            server.enqueue(MockResponse().setResponseCode(204)); transport.send(request)
            server.enqueue(MockResponse().setResponseCode(403))
            assertEquals(403, assertFailsWith<HttpStatusException> { transport.send(request) }.statusCode)
            server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START))
            assertFailsWith<IOException> { withTimeout(5000) { transport.send(request) } }
            server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE))
            val call = launch { transport.send(request) }
            delay(100); call.cancelAndJoin()
        } finally { transport.close(); server.shutdown() }
        assertTrue(transport.client.dispatcher.executorService.isShutdown)
    }

    @Test fun `injected HTTP client remains owned by its caller`() {
        val client = OkHttpClient()
        val transport = OkHttpTransport(client)
        transport.close()
        assertFalse(client.dispatcher.executorService.isShutdown)
        client.dispatcher.executorService.shutdown(); client.connectionPool.evictAll()
    }

    @Test fun `public defaults and lifecycle constructor open anonymous SSE and reconnect after EOF`() = runBlocking {
        for (bound in listOf(false, true)) {
            val server = MockWebServer(); server.start()
            server.enqueue(MockResponse().setHeader("Content-Type", "text/event-stream").setBody(": ping\n\n"))
            server.enqueue(MockResponse().setHeader("Content-Type", "text/event-stream").setBody(": ping\n\n"))
            val owner = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            val client = if (bound) TransmitClient(server.url("/").toString(), lifecycleScope = owner)
                else TransmitClient(server.url("/").toString())
            try {
                client.connect()
                val first = server.takeRequest(5, TimeUnit.SECONDS)!!
                val second = server.takeRequest(5, TimeUnit.SECONDS)!!
                assertEquals(first.requestUrl!!.encodedPath, second.requestUrl!!.encodedPath)
                assertNotEquals(first.requestUrl!!.queryParameter("uid"), second.requestUrl!!.queryParameter("uid"))
                assertNull(first.getHeader("Authorization"))
            } finally {
                if (bound) owner.cancel() else client.close()
                client.awaitClosed(); owner.cancel(); server.shutdown()
            }
            assertEquals(TransmitClient.State.Closed, client.state.value)
        }
    }

    @Test fun `lifecycle binding requires a cancellable owner`() {
        val owner = object : CoroutineScope {
            override val coroutineContext = kotlin.coroutines.EmptyCoroutineContext
        }
        assertFailsWith<IllegalArgumentException> { TransmitClient("http://localhost", lifecycleScope = owner) }
    }

    @Test fun `public defaults and immediate close settle`() = runBlocking {
        val client = TransmitClient("http://localhost")
        client.close(); client.awaitClosed()
        assertEquals(TransmitClient.State.Closed, client.state.value)
    }
}
