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
    internal val client = client ?: OkHttpClient.Builder().readTimeout(90, TimeUnit.SECONDS).build()
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

internal fun parseEnvelope(data: String): Pair<String, JsonElement>? {
    val envelope = try { Json.parseToJsonElement(data) as? JsonObject } catch (_: IllegalArgumentException) { null }
    val channel = envelope?.get("channel") as? JsonPrimitive
    val payload = envelope?.get("payload")
    return if (channel != null && channel.isString && payload != null) channel.content to payload else null
}
