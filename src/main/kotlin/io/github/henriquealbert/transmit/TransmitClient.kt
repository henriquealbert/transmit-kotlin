package io.github.henriquealbert.transmit

import java.io.IOException
import java.util.UUID
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.decodeFromJsonElement
import okhttp3.OkHttpClient
import okhttp3.Response
import okhttp3.sse.EventSource
import okhttp3.sse.EventSourceListener

/** AdonisJS Transmit client. Retain while collecting messages and explicitly [close] its owner. */
class TransmitClient internal constructor(
    private val requests: Requests,
    private val transport: Transport,
    dispatcher: CoroutineDispatcher,
    private val waitForRetry: suspend (Long) -> Unit,
) : AutoCloseable {
    constructor(
        baseUrl: String,
        tokenQueryParameter: String? = null,
        client: OkHttpClient? = null,
        tokenProvider: suspend () -> String? = { null },
    ) : this(Requests(baseUrl, tokenQueryParameter, tokenProvider), OkHttpTransport(client), Dispatchers.IO, { delay(it) })

    enum class State { Idle, Connecting, Connected, Reconnecting, AuthenticationFailed, Closed }

    /** Stable for this instance across connection loss and token refresh. */
    val uid: String = UUID.randomUUID().toString()
    private val mutableState = MutableStateFlow(State.Idle)
    val state: StateFlow<State> = mutableState.asStateFlow()
    private val mutableError = MutableStateFlow<Throwable?>(null)
    val lastError: StateFlow<Throwable?> = mutableError.asStateFlow()
    private val job = SupervisorJob()
    private val scope = CoroutineScope(job + dispatcher)
    private val commands = Channel<suspend () -> Unit>(Channel.UNLIMITED)
    private val channels = mutableMapOf<String, MutableMap<UUID, Channel<JsonElement>>>()
    private val subscribed = mutableSetOf<String>()
    private var source: EventSource? = null
    private var retry: Job? = null
    private var generation = 0L
    private var retryMillis = 1_000L
    private var authenticationFailures = 0
    private val worker = scope.launch(start = CoroutineStart.UNDISPATCHED) {
        try {
            for (command in commands) command()
        } finally {
            source?.cancel()
            for (channel in channels.keys.toList()) finish(channel)
            commands.cancel()
            transport.close()
        }
    }

    /** Schedules opening without subscribing; observe [state] for readiness. */
    fun connect() { commands.trySend { start() } }

    /** A cold flow: every collector is independent; collectors of a channel share one server subscription. */
    fun messages(channel: String): Flow<JsonElement> = flow {
        if (state.value == State.Closed) throw ClientClosedException()
        val id = UUID.randomUUID()
        val sink = Channel<JsonElement>(Channel.UNLIMITED)
        val completion = job.invokeOnCompletion { sink.close() }
        commands.trySend {
            if (state.value == State.AuthenticationFailed) sink.close(lastError.value)
            else {
                channels.getOrPut(channel) { mutableMapOf() }[id] = sink
                if (state.value == State.Connected) reconcile(channel)
                start()
            }
        }
        try { for (payload in sink) emit(payload) }
        finally {
            completion.dispose()
            sink.cancel()
            commands.trySend {
                channels[channel]?.remove(id)
                if (channels[channel]?.isEmpty() == true) {
                    channels.remove(channel)
                    reconcile(channel)
                }
            }
        }
    }

    inline fun <reified T> messages(channel: String, json: Json = Json): Flow<T> =
        messages(channel).map { json.decodeFromJsonElement<T>(it) }

    /** Ends all consumers of a channel. Cancelling one collector only removes that collector. */
    fun unsubscribe(channel: String) { commands.trySend { finish(channel); reconcile(channel) } }

    /** Reopens with the provider's latest credentials. Register new collectors after AuthenticationFailed. */
    fun refreshAuthentication() {
        commands.trySend {
            if (state.value == State.AuthenticationFailed) {
                authenticationFailures = 0
                transition(State.Idle)
                mutableError.value = null
                start()
            } else scheduleRetry()
        }
    }

    /** Permanent, idempotent cancellation. Never shuts down a caller-supplied OkHttpClient. */
    override fun close() {
        mutableState.value = State.Closed
        scope.cancel()
    }

    /** Waits for cancelled requests, consumers, and the owned controller to settle. */
    suspend fun awaitClosed() { worker.join() }

    private fun transition(next: State) {
        mutableState.update { current -> if (current == State.Closed) current else next }
    }

    private suspend fun start() {
        if (source != null || retry != null || state.value == State.AuthenticationFailed) return
        transition(if (state.value == State.Idle) State.Connecting else State.Reconnecting)
        val epoch = ++generation
        try {
            val request = requests.create("events", uid)
            source = transport.open(request, object : EventSourceListener() {
                override fun onOpen(eventSource: EventSource, response: Response) { enqueue(epoch) {
                    authenticationFailures = 0
                    mutableError.value = null
                    transition(State.Connected)
                    for (channel in channels.keys.toList()) reconcile(channel)
                } }
                override fun onEvent(eventSource: EventSource, id: String?, type: String?, data: String) { enqueue(epoch) {
                    val event = parseEnvelope(data)
                    if (event != null) {
                        retryMillis = 1_000
                        for (sink in channels[event.first].orEmpty().values) sink.trySend(event.second)
                    }
                } }
                override fun onClosed(eventSource: EventSource) { enqueue(epoch) { scheduleRetry() } }
                override fun onFailure(eventSource: EventSource, t: Throwable?, response: Response?) { enqueue(epoch) {
                    failed(if (response != null && !response.isSuccessful) HttpStatusException(response.code)
                        else t ?: IOException("SSE stream failed"))
                } }
            })
        } catch (error: Exception) {
            if (error is CancellationException) throw error
            failed(error)
        }
    }

    private fun enqueue(epoch: Long, action: suspend () -> Unit) {
        commands.trySend { if (epoch == generation) action() }
    }

    private fun failed(error: Throwable) {
        mutableError.value = error
        authenticationFailures = if (error is HttpStatusException && error.statusCode in listOf(401, 403)) authenticationFailures + 1 else 0
        if (authenticationFailures >= 3) {
            disconnect()
            transition(State.AuthenticationFailed)
            for (channel in channels.keys.toList()) finish(channel, error)
        } else scheduleRetry()
    }

    private fun disconnect() {
        generation++
        source?.cancel()
        source = null
        retry?.cancel()
        retry = null
        subscribed.clear()
    }

    private fun scheduleRetry() {
        disconnect()
        transition(State.Reconnecting)
        val wait = retryMillis
        retryMillis = (retryMillis * 2).coerceAtMost(30_000)
        retry = scope.launch {
            waitForRetry(wait)
            commands.send { retry = null; start() }
        }
    }

    private suspend fun reconcile(channel: String) {
        if (state.value != State.Connected) return
        if (channel in channels && channel !in subscribed) {
            try {
                transport.send(requests.create("subscribe", uid, channel))
                subscribed.add(channel)
            } catch (error: Exception) {
                if (error is CancellationException) throw error
                finish(channel, error)
            }
        } else if (channel !in channels && subscribed.remove(channel)) {
            try { transport.send(requests.create("unsubscribe", uid, channel)) }
            catch (error: Exception) { if (error is CancellationException) throw error }
        }
    }

    private fun finish(channel: String, error: Throwable? = null) {
        channels.remove(channel)?.values?.forEach { it.close(error) }
    }
}
