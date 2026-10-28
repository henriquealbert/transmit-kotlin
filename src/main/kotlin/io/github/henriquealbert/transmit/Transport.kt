package io.github.henriquealbert.transmit

import java.io.IOException
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.serialization.json.*
import okhttp3.*
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.sse.EventSource
import okhttp3.sse.EventSourceListener
import okhttp3.sse.EventSources

/** A non-successful HTTP response, including authentication and authorization failures. */
class HttpStatusException(val statusCode: Int) : IOException("HTTP $statusCode")
class ClientClosedException : IllegalStateException("Transmit client is closed")

internal class Requests(baseUrl: String, private val queryKey: String?, private val token: suspend () -> String?) {
    private val base = baseUrl.toHttpUrl().also {
        require(it.username.isEmpty() && it.password.isEmpty() && it.query == null && it.fragment == null) {
            "Base URL must not contain credentials, a query, or a fragment"
        }
    }

    suspend fun create(action: String, uid: String, channel: String? = null): Request {
        val credential = token()
        val url = base.newBuilder().addPathSegments("__transmit/$action")
        if (channel == null) url.addQueryParameter("uid", uid)
        if (queryKey != null && credential != null) url.addQueryParameter(queryKey, credential)
        return Request.Builder().url(url.build()).apply {
            if (channel == null) header("Accept", "text/event-stream")
            if (credential != null) header("Authorization", "Bearer $credential")
            if (channel != null) post(buildJsonObject {
                put("uid", uid)
                put("channel", channel)
            }.toString().toRequestBody("application/json".toMediaType()))
        }.build()
    }
}

internal interface Transport {
    fun open(request: Request, listener: EventSourceListener): EventSource
    suspend fun send(request: Request)
    fun close()
}

internal class OkHttpTransport(client: OkHttpClient?) : Transport {
    private val owned = client == null
    internal val client = when {
        client == null -> OkHttpClient.Builder().readTimeout(90, TimeUnit.SECONDS)
            .cookieJar(InMemoryCookieJar()).build()
        client.cookieJar === CookieJar.NO_COOKIES -> client.newBuilder().cookieJar(InMemoryCookieJar()).build()
        else -> client
    }
    override fun open(request: Request, listener: EventSourceListener): EventSource =
        EventSources.createFactory(client).newEventSource(request, listener)

    override suspend fun send(request: Request): Unit = suspendCancellableCoroutine { continuation ->
        val call = client.newCall(request)
        continuation.invokeOnCancellation { call.cancel() }
        call.enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) { continuation.resumeWithException(e) }
            override fun onResponse(call: Call, response: Response) {
                response.use {
                    if (it.isSuccessful) continuation.resume(Unit)
                    else continuation.resumeWithException(HttpStatusException(it.code))
                }
            }
        })
    }

    override fun close() {
        if (owned) {
            client.dispatcher.executorService.shutdown()
            client.connectionPool.evictAll()
        }
    }
}

/** Session-only storage shared by this transport's stream and subscription requests. */
internal class InMemoryCookieJar : CookieJar {
    private val cookies = linkedMapOf<Triple<String, String, String>, Cookie>()

    @Synchronized override fun saveFromResponse(url: HttpUrl, cookies: List<Cookie>) {
        val now = System.currentTimeMillis()
        this.cookies.values.removeAll { it.expiresAt <= now }
        for (cookie in cookies) {
            val key = Triple(cookie.name, cookie.domain, cookie.path)
            if (cookie.expiresAt <= now) this.cookies.remove(key)
            else this.cookies[key] = cookie
        }
    }

    @Synchronized override fun loadForRequest(url: HttpUrl): List<Cookie> {
        cookies.values.removeAll { it.expiresAt <= System.currentTimeMillis() }
        return cookies.values.filter { it.matches(url) }.sortedByDescending { it.path.length }
    }
}

internal fun parseEnvelope(data: String): Pair<String, JsonElement>? {
    val envelope = try { Json.parseToJsonElement(data) as? JsonObject } catch (_: IllegalArgumentException) { null }
    val channel = envelope?.get("channel") as? JsonPrimitive
    val payload = envelope?.get("payload")
    return if (channel != null && channel.isString && payload != null) channel.content to payload else null
}
