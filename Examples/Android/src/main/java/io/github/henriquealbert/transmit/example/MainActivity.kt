package io.github.henriquealbert.transmit.example

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.unit.dp
import androidx.lifecycle.lifecycleScope
import io.github.henriquealbert.transmit.TransmitClient
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody

@Serializable
data class Message(val id: String, val text: String, val source: String, val serverInstance: String, val sequence: Int, val sentAt: String)

class MainActivity : ComponentActivity() {
    private val baseUrl = "http://10.0.2.2:3333"
    private val client = TransmitClient(baseUrl)
    private val http = OkHttpClient()
    private val messages = mutableStateListOf<Message>()
    private var reconnects by mutableIntStateOf(0)
    private var error by mutableStateOf<String?>(null)

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        lifecycleScope.launch {
            var wasConnected = false
            client.state.collect {
                if (it == TransmitClient.State.Connected) {
                    if (wasConnected) reconnects++
                    wasConnected = true
                }
            }
        }
        lifecycleScope.launch {
            val seen = mutableSetOf<String>()
            try {
                client.messages<Message>("demo/messages").collect {
                    if (seen.add(it.id)) {
                        messages.add(0, it)
                        if (messages.size > 100) messages.removeAt(messages.lastIndex)
                    }
                }
            } catch (e: Exception) { error = e.message }
        }
        setContent {
            val state by client.state.collectAsState()
            val listState = rememberLazyListState()
            LaunchedEffect(messages.size) { listState.scrollToItem(0) }
            MaterialTheme(colorScheme = darkColorScheme(primary = Color(0xFF72E2BE), background = Color(0xFF101B27), surface = Color(0xFF1C2C3D))) {
                Surface(Modifier.fillMaxSize()) {
                    Column(Modifier.fillMaxSize().safeDrawingPadding().padding(24.dp), verticalArrangement = Arrangement.spacedBy(16.dp)) {
                        Text("TRANSMIT / ANDROID", style = MaterialTheme.typography.labelLarge, color = MaterialTheme.colorScheme.primary)
                        Text("Live from Adonis", style = MaterialTheme.typography.headlineLarge)
                        Text("${state.name}  •  ${messages.size} messages  •  $reconnects reconnects", color = MaterialTheme.colorScheme.primary)
                        Text("One stream. Shared channels. Automatic recovery.", style = MaterialTheme.typography.bodyMedium)
                        Button(onClick = { broadcast() }, modifier = Modifier.fillMaxWidth()) { Text("Send a message") }
                        error?.let { Text(it, color = MaterialTheme.colorScheme.error) }
                        LazyColumn(state = listState, verticalArrangement = Arrangement.spacedBy(12.dp)) {
                            items(messages, key = { it.id }) { message ->
                                Card(Modifier.fillMaxWidth()) {
                                    Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                                        Text(message.text, style = MaterialTheme.typography.titleMedium)
                                        Text("${message.source.uppercase()}  /  #${message.sequence}  /  ${message.serverInstance}", style = MaterialTheme.typography.labelSmall)
                                        Text(message.sentAt, style = MaterialTheme.typography.bodySmall)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private fun broadcast() { lifecycleScope.launch {
        try {
            withContext(Dispatchers.IO) {
                val body = buildJsonObject { put("text", "Hello from Android") }.toString()
                http.newCall(Request.Builder().url("$baseUrl/broadcast").post(body.toRequestBody("application/json".toMediaType())).build()).execute().use {
                    check(it.isSuccessful) { "Broadcast failed: ${it.code}" }
                }
            }
        } catch (e: Exception) { error = e.message }
    } }

    override fun onDestroy() {
        client.close()
        http.dispatcher.cancelAll()
        http.dispatcher.executorService.shutdown()
        http.connectionPool.evictAll()
        super.onDestroy()
    }
}
