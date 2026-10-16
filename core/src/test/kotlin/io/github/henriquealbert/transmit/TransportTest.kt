package io.github.henriquealbert.transmit

import java.io.IOException
import java.util.concurrent.TimeUnit
import kotlin.test.*
import kotlinx.coroutines.*
import okhttp3.*
import okhttp3.mockwebserver.*
import org.junit.Test

class TransportTest {
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

    @Test fun `public defaults open anonymous SSE and reconnect after EOF`() = runBlocking {
        val server = MockWebServer(); server.start()
        server.enqueue(MockResponse().setHeader("Content-Type", "text/event-stream").setBody(": ping\n\n"))
        server.enqueue(MockResponse().setHeader("Content-Type", "text/event-stream").setBody(": ping\n\n"))
        val client = TransmitClient(server.url("/").toString())
        try {
            client.connect()
            val first = server.takeRequest(5, TimeUnit.SECONDS)!!
            val second = server.takeRequest(5, TimeUnit.SECONDS)!!
            assertEquals(first.path, second.path)
            assertNull(first.getHeader("Authorization"))
        } finally { client.close(); client.awaitClosed(); server.shutdown() }
    }

    @Test fun `public defaults and immediate close settle`() = runBlocking {
        val client = TransmitClient("http://localhost")
        client.close(); client.awaitClosed()
        assertEquals(TransmitClient.State.Closed, client.state.value)
    }
}
