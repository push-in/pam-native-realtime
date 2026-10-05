package dev.pam.realtime

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import dev.pam.nativeapp.modules.ModuleCompletion
import dev.pam.nativeapp.protocol.WireValue
import java.io.IOException
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.math.pow
import kotlin.random.Random
import okhttp3.Call
import okhttp3.Callback
import okhttp3.FormBody
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONArray
import org.json.JSONObject

/** Configuration decoded from the PHP `PusherConnector`. */
internal class PusherConfig(
    val url: String,
    val headers: Map<String, String>,
    val authEndpoint: String?,
    val authHeaders: Map<String, String>,
    @Volatile var bearer: String?,
    val backoffInitialMs: Long,
    val backoffMaxMs: Long,
    val backoffMultiplier: Double,
    val activityTimeoutMs: Long,
    val pongTimeoutMs: Long,
    val handshakeTimeoutMs: Long,
    val terminalRetryMs: Long,
    val quietMs: Long,
    val maxDelayMs: Long,
    val capacity: Int,
    val maxBatch: Int,
) {
    companion object {
        fun parse(json: String): PusherConfig {
            val o = JSONObject(json)
            val url = o.getString("url")
            require(url.startsWith("wss://") || url.startsWith("ws://")) { "Pusher URL must use wss://" }
            return PusherConfig(
                url = url,
                headers = o.optJSONObject("headers").toStringMap(),
                authEndpoint = o.optString("authEndpoint").takeIf { it.isNotEmpty() },
                authHeaders = o.optJSONObject("authHeaders").toStringMap(),
                bearer = o.optString("bearer").takeIf { it.isNotEmpty() },
                backoffInitialMs = o.optLong("backoffInitialMs", 400).coerceIn(50, 60_000),
                backoffMaxMs = o.optLong("backoffMaxMs", 15_000).coerceIn(100, 600_000),
                backoffMultiplier = o.optDouble("backoffMultiplier", 1.8).coerceIn(1.0, 10.0),
                activityTimeoutMs = o.optLong("activityTimeoutMs", 30_000).coerceIn(1_000, 600_000),
                pongTimeoutMs = o.optLong("pongTimeoutMs", 12_000).coerceIn(500, 120_000),
                handshakeTimeoutMs = o.optLong("handshakeTimeoutMs", 10_000).coerceIn(1_000, 120_000),
                terminalRetryMs = o.optLong("terminalRetryMs", 300_000).coerceIn(1_000, 86_400_000),
                quietMs = o.optLong("quietMs", 32).coerceIn(0, 1_000),
                maxDelayMs = o.optLong("maxDelayMs", 150).coerceIn(0, 5_000),
                capacity = o.optInt("capacity", 2_048).coerceIn(16, 65_536),
                maxBatch = o.optInt("maxBatch", 512).coerceIn(1, 8_192),
            )
        }

        private fun JSONObject?.toStringMap(): Map<String, String> {
            if (this == null) return emptyMap()
            val out = LinkedHashMap<String, String>()
            keys().forEach { out[it] = getString(it) }
            return out
        }
    }
}

/**
 * One Pusher-protocol (Pusher Channels / Laravel Reverb / Soketi) connection.
 *
 * All mutable state is confined to a private single-threaded scheduler; OkHttp
 * and connectivity callbacks hop onto it. PHP never polls: every observable
 * change is offered to [events], which releases coalesced batches to the one
 * pending `pusherNext` completion.
 */
internal class PusherClient(
    val id: String,
    private val config: PusherConfig,
    http: OkHttpClient,
    private val context: Context?,
) {
    private class ChannelState(val name: String) {
        var subscribed = false
        var authorizing = false
        var authAttempt = 0
        var retry: ScheduledFuture<*>? = null
        val presence get() = name.startsWith("presence-")
        val requiresAuth get() = name.startsWith("private-") || presence
    }

    private class PendingWhisper(var data: Any?, var task: ScheduledFuture<*>?)

    private val executor = ScheduledThreadPoolExecutor(1) { runnable ->
        Thread(runnable, "pam-realtime-$id").apply { isDaemon = true }
    }.apply {
        executeExistingDelayedTasksAfterShutdownPolicy = false
        continueExistingPeriodicTasksAfterShutdownPolicy = false
        removeOnCancelPolicy = true
    }
    private val http = http.newBuilder()
        .pingInterval(0, TimeUnit.MILLISECONDS)
        .readTimeout(0, TimeUnit.MILLISECONDS)
        .connectTimeout(config.handshakeTimeoutMs, TimeUnit.MILLISECONDS)
        .build()
    val events = EventBatcher(executor, config.quietMs, maxOf(config.quietMs, config.maxDelayMs), config.capacity, config.maxBatch)

    private val channels = LinkedHashMap<String, ChannelState>()
    private val lastWhisperAt = HashMap<String, Long>()
    private val pendingWhispers = HashMap<String, PendingWhisper>()
    private val journal = ArrayDeque<JSONObject>()
    private var socket: WebSocket? = null
    private var generation = 0L
    private var state = STATE_DISCONNECTED
    private var reason = ""
    private var socketId: String? = null
    private var attempt = 0
    private var retryAt = 0L
    private var retryNotBefore = 0L
    private var activityTimeoutMs = config.activityTimeoutMs
    private var lastFrameAt = 0L
    private var pingSentAt = 0L
    private var connectedAt = 0L
    private var lastError = ""
    private var reconnectTask: ScheduledFuture<*>? = null
    private var handshakeTask: ScheduledFuture<*>? = null
    private var livenessTask: ScheduledFuture<*>? = null
    private var networkCallback: ConnectivityManager.NetworkCallback? = null
    @Volatile private var closed = false

    fun start() = post {
        registerNetworkCallback()
        openSocket("connect")
    }

    fun subscribe(name: String) = post {
        if (channels.containsKey(name)) return@post
        val channel = ChannelState(name)
        channels[name] = channel
        subscribeChannel(channel)
    }

    fun unsubscribe(name: String) = post {
        val channel = channels.remove(name) ?: return@post
        channel.retry?.cancel(false)
        pendingWhispers.keys.filter { it.startsWith("$name\u0000") }.forEach { pendingWhispers.remove(it)?.task?.cancel(false) }
        if (channel.subscribed || channel.authorizing) {
            send(JSONObject().put("event", "pusher:unsubscribe").put("data", JSONObject().put("channel", name)))
        }
        record("Left $name")
    }

    fun whisper(channelName: String, event: String, data: String, completion: ModuleCompletion) = post {
        val channel = channels[channelName]
        if (channel == null || !channel.subscribed || !channel.requiresAuth || state != STATE_CONNECTED) {
            completion.success(mapOf("sent" to WireValue.Flag(false)))
            return@post
        }
        val payload = parseData(data)
        val key = "$channelName\u0000$event"
        val now = now()
        val elapsed = now - (lastWhisperAt[key] ?: 0L)
        val pending = pendingWhispers[key]
        if (pending == null && elapsed >= WHISPER_INTERVAL_MS) {
            lastWhisperAt[key] = now
            send(JSONObject().put("event", event).put("channel", channelName).put("data", payload))
        } else if (pending != null) {
            // Trailing-edge throttle: only the newest payload of a burst is sent.
            pending.data = payload
        } else {
            val entry = PendingWhisper(payload, null)
            pendingWhispers[key] = entry
            entry.task = executor.schedule({
                pendingWhispers.remove(key)
                val target = channels[channelName]
                if (!closed && target?.subscribed == true && state == STATE_CONNECTED) {
                    lastWhisperAt[key] = now()
                    send(JSONObject().put("event", event).put("channel", channelName).put("data", entry.data))
                }
            }, WHISPER_INTERVAL_MS - elapsed, TimeUnit.MILLISECONDS)
        }
        completion.success(mapOf("sent" to WireValue.Flag(true)))
    }

    fun token(bearer: String?) = post {
        config.bearer = bearer
        // A refreshed credential usually fixes 401/403 channel auth: retry now.
        channels.values.filter { !it.subscribed && !it.authorizing && it.retry != null }.forEach {
            it.retry?.cancel(false)
            it.retry = null
            it.authAttempt = 0
            subscribeChannel(it)
        }
    }

    fun reconnect() = post {
        attempt = 0
        retryNotBefore = 0
        closeSocket(1000, "reconnect")
        openSocket("reconnect")
    }

    fun status(completion: ModuleCompletion) = post {
        completion.success(mapOf("status" to WireValue.Text(snapshot().toString())))
    }

    fun close() {
        if (closed) return
        post {
            closed = true
            unregisterNetworkCallback()
            reconnectTask?.cancel(false)
            pendingWhispers.values.forEach { it.task?.cancel(false) }
            pendingWhispers.clear()
            channels.values.forEach { it.retry?.cancel(false) }
            channels.clear()
            closeSocket(1000, "disconnect")
            state = STATE_DISCONNECTED
            events.close()
            executor.shutdown()
        }
    }

    // ------------------------------------------------------------------ socket

    private fun openSocket(cause: String) {
        if (closed) return
        reconnectTask?.cancel(false)
        reconnectTask = null
        retryAt = 0
        val current = ++generation
        socketId = null
        setState(STATE_CONNECTING, cause)
        record("Connecting ($cause)")
        val request = Request.Builder().url(config.url).apply {
            config.headers.forEach { (name, value) -> header(name, value) }
        }.build()
        socket = http.newWebSocket(request, Listener(current))
        handshakeTask?.cancel(false)
        handshakeTask = executor.schedule({
            if (current == generation && state == STATE_CONNECTING) lost(current, "Handshake timed out")
        }, config.handshakeTimeoutMs, TimeUnit.MILLISECONDS)
    }

    private inner class Listener(private val owner: Long) : WebSocketListener() {
        override fun onMessage(webSocket: WebSocket, text: String) = post { if (owner == generation) frame(text) }

        override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
            webSocket.close(1000, null)
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) = post {
            if (owner == generation) lost(owner, reason.ifBlank { "Closed ($code)" }, code)
        }

        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) = post {
            if (owner == generation) lost(owner, t.message ?: t.javaClass.simpleName)
        }
    }

    private fun closeSocket(code: Int, why: String) {
        generation++
        handshakeTask?.cancel(false)
        livenessTask?.cancel(false)
        socket?.close(code, why)
        socket = null
        socketId = null
        pingSentAt = 0
        resetChannels()
    }

    /** The current socket is gone: forget it and schedule a reconnect. */
    private fun lost(owner: Long, why: String, code: Int = 0, immediate: Boolean = false) {
        if (owner != generation || closed) return
        lastError = why
        record("Connection lost: $why")
        socket?.cancel()
        closeSocket(1000, "lost")
        scheduleReconnect(why, immediate || code in 4200..4299)
    }

    private fun scheduleReconnect(why: String, immediate: Boolean) {
        if (closed) return
        reconnectTask?.cancel(false)
        val now = now()
        val terminal = retryNotBefore - now
        val delay: Long
        if (terminal > 0) {
            delay = terminal
            retryAt = now + delay
            setState(STATE_FAILED, why)
        } else {
            attempt++
            delay = if (immediate) 0 else backoff(attempt, config.backoffInitialMs, config.backoffMaxMs, config.backoffMultiplier)
            retryAt = now + delay
            setState(STATE_UNAVAILABLE, why)
        }
        reconnectTask = executor.schedule({
            reconnectTask = null
            openSocket("retry")
        }, delay, TimeUnit.MILLISECONDS)
    }

    private fun backoff(n: Int, floor: Long, ceiling: Long, multiplier: Double): Long {
        val cap = minOf(ceiling.toDouble(), floor * multiplier.pow((n - 1).coerceAtMost(30))).toLong().coerceAtLeast(floor)
        return if (cap <= floor) floor else Random.nextLong(floor, cap + 1)
    }

    // ------------------------------------------------------------------ frames

    private fun frame(text: String) {
        lastFrameAt = now()
        pingSentAt = 0
        val message = runCatching { JSONObject(text) }.getOrElse {
            record("Ignored a malformed frame")
            return
        }
        val event = message.optString("event")
        val channel = if (message.has("channel") && !message.isNull("channel")) message.optString("channel") else null
        when (event) {
            "pusher:connection_established" -> established(objectData(message))
            "pusher:error" -> serverError(objectData(message))
            "pusher:ping" -> send(JSONObject().put("event", "pusher:pong").put("data", JSONObject()))
            "pusher:pong" -> Unit
            "pusher_internal:subscription_succeeded" -> channel?.let { subscribed(it, objectData(message)) }
            "pusher:subscription_error" -> channel?.let { name ->
                val data = objectData(message)
                channels[name]?.let { failed(it, data.optInt("status", 0), data.optString("error", "Subscription rejected")) }
            }
            "pusher_internal:member_added" -> channel?.takeIf { channels[it]?.subscribed == true }?.let {
                val data = objectData(message)
                events.offer(null, JSONObject().put("k", KIND_MEMBER_ADDED).put("c", it).put("u", data.opt("user_id")?.toString().orEmpty()).put("d", data.opt("user_info")?.toString() ?: "null"))
            }
            "pusher_internal:member_removed" -> channel?.takeIf { channels[it]?.subscribed == true }?.let {
                val data = objectData(message)
                events.offer(null, JSONObject().put("k", KIND_MEMBER_REMOVED).put("c", it).put("u", data.opt("user_id")?.toString().orEmpty()))
            }
            else -> {
                if (channel == null || event.isEmpty() || event.startsWith("pusher")) return
                if (channels[channel] == null) return
                val raw = when (val data = message.opt("data")) {
                    null, JSONObject.NULL -> ""
                    is String -> data
                    else -> data.toString()
                }
                val user = message.opt("user_id")?.takeIf { it != JSONObject.NULL }?.toString().orEmpty()
                val whisper = event.startsWith("client-")
                val entry = JSONObject().put("k", if (whisper) KIND_WHISPER else KIND_MESSAGE).put("c", channel).put("n", event).put("d", raw).put("u", user)
                events.offer(if (whisper) "w\u0000$channel\u0000$event\u0000$user" else null, entry)
            }
        }
    }

    private fun established(data: JSONObject) {
        val id = data.optString("socket_id")
        if (id.isEmpty()) {
            lost(generation, "Handshake without socket id")
            return
        }
        handshakeTask?.cancel(false)
        socketId = id
        val serverActivity = data.optLong("activity_timeout", 0) * 1_000
        activityTimeoutMs = if (serverActivity > 0) minOf(serverActivity, config.activityTimeoutMs) else config.activityTimeoutMs
        attempt = 0
        retryAt = 0
        retryNotBefore = 0
        lastError = ""
        connectedAt = System.currentTimeMillis()
        record("Connected as $id")
        setState(STATE_CONNECTED, "")
        startLiveness()
        channels.values.forEach { subscribeChannel(it) }
    }

    private fun serverError(data: JSONObject) {
        val code = if (data.has("code") && !data.isNull("code")) data.optInt("code", 0) else 0
        val message = data.optString("message").ifBlank { "Pusher error" }
        lastError = if (code > 0) "$message ($code)" else message
        record("Server error: $lastError")
        when (code) {
            in 4000..4099 -> {
                retryNotBefore = now() + config.terminalRetryMs
                lost(generation, lastError, code)
            }
            in 4100..4299 -> lost(generation, lastError, code)
            else -> events.offer("error", stateEntry().put("r", lastError))
        }
    }

    private fun subscribed(name: String, data: JSONObject) {
        val channel = channels[name] ?: return
        channel.subscribed = true
        channel.authorizing = false
        channel.authAttempt = 0
        channel.retry?.cancel(false)
        channel.retry = null
        record("Subscribed $name")
        val entry = JSONObject().put("k", KIND_SUBSCRIBED).put("c", name)
        if (channel.presence) {
            val presence = data.optJSONObject("presence") ?: JSONObject()
            entry.put("m", (presence.optJSONObject("hash") ?: JSONObject()).toString())
        }
        events.offer("s\u0000$name", entry)
    }

    // --------------------------------------------------------------- channels

    private fun resetChannels() {
        channels.values.forEach {
            it.subscribed = false
            it.authorizing = false
            it.retry?.cancel(false)
            it.retry = null
        }
    }

    private fun subscribeChannel(channel: ChannelState) {
        val sid = socketId
        if (closed || state != STATE_CONNECTED || sid == null || channel.subscribed || channel.authorizing) return
        if (!channel.requiresAuth) {
            send(JSONObject().put("event", "pusher:subscribe").put("data", JSONObject().put("channel", channel.name)))
            return
        }
        val endpoint = config.authEndpoint
        if (endpoint == null) {
            events.offer("e\u0000${channel.name}", JSONObject().put("k", KIND_SUBSCRIPTION_FAILED).put("c", channel.name).put("h", 0).put("r", "No auth endpoint configured").put("w", -1))
            return
        }
        channel.authorizing = true
        val owner = generation
        val body = FormBody.Builder().add("socket_id", sid).add("channel_name", channel.name).build()
        val request = Request.Builder().url(endpoint).post(body).header("Accept", "application/json").apply {
            config.authHeaders.forEach { (name, value) -> header(name, value) }
            config.bearer?.let { header("Authorization", "Bearer $it") }
        }.build()
        http.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) = post { authorized(channel.name, owner, sid, 0, null, e.message ?: "Auth request failed") }

            override fun onResponse(call: Call, response: Response) {
                val code = response.code
                val text = runCatching { response.use { it.body.string() } }.getOrNull()
                post { authorized(channel.name, owner, sid, code, text, "Auth HTTP $code") }
            }
        })
    }

    private fun authorized(name: String, owner: Long, sid: String, code: Int, body: String?, error: String) {
        val channel = channels[name] ?: return
        if (owner != generation || socketId != sid) {
            channel.authorizing = false
            return
        }
        channel.authorizing = false
        val auth = if (code in 200..299 && body != null) runCatching { JSONObject(body) }.getOrNull()?.takeIf { it.has("auth") } else null
        if (auth == null) {
            failed(channel, code, if (code in 200..299) "Invalid auth response" else error)
            return
        }
        auth.put("channel", name)
        send(JSONObject().put("event", "pusher:subscribe").put("data", auth))
    }

    private fun failed(channel: ChannelState, status: Int, message: String) {
        channel.subscribed = false
        channel.authorizing = false
        channel.authAttempt++
        val delay = if (status == 401 || status == 403) FORBIDDEN_RETRY_MS else backoff(channel.authAttempt, AUTH_RETRY_MIN_MS, AUTH_RETRY_MAX_MS, 2.0)
        record("Subscription to ${channel.name} failed: $message")
        events.offer(
            "e\u0000${channel.name}",
            JSONObject().put("k", KIND_SUBSCRIPTION_FAILED).put("c", channel.name).put("h", status).put("r", message).put("w", delay),
        )
        channel.retry?.cancel(false)
        channel.retry = executor.schedule({
            channel.retry = null
            if (channels[channel.name] === channel) subscribeChannel(channel)
        }, delay, TimeUnit.MILLISECONDS)
    }

    // --------------------------------------------------------------- liveness

    private fun startLiveness() {
        livenessTask?.cancel(false)
        val period = (activityTimeoutMs / 6).coerceIn(250, 5_000)
        livenessTask = executor.scheduleWithFixedDelay({ checkLiveness() }, period, period, TimeUnit.MILLISECONDS)
    }

    private fun checkLiveness() {
        if (state != STATE_CONNECTED || closed) return
        val now = now()
        if (pingSentAt > 0 && now - pingSentAt >= config.pongTimeoutMs) {
            lost(generation, "Pong timeout")
        } else if (pingSentAt == 0L && now - lastFrameAt >= activityTimeoutMs) {
            probe()
        }
    }

    private fun probe() {
        if (state != STATE_CONNECTED || pingSentAt > 0) return
        pingSentAt = now()
        send(JSONObject().put("event", "pusher:ping").put("data", JSONObject()))
    }

    private fun registerNetworkCallback() {
        val manager = context?.getSystemService(ConnectivityManager::class.java) ?: return
        val callback = object : ConnectivityManager.NetworkCallback() {
            private var known: Network? = null

            override fun onAvailable(network: Network) = post {
                val changed = known != null && known != network
                known = network
                when {
                    // A backoff wait is stale evidence once the network is back.
                    state == STATE_UNAVAILABLE -> openSocket("network")
                    state == STATE_CONNECTED && changed -> probe()
                }
            }

            override fun onLost(network: Network) = post {
                if (known == network) known = null
                if (state == STATE_CONNECTED) probe()
            }
        }
        runCatching { manager.registerDefaultNetworkCallback(callback) }.onSuccess { networkCallback = callback }
    }

    private fun unregisterNetworkCallback() {
        val callback = networkCallback ?: return
        networkCallback = null
        runCatching { context?.getSystemService(ConnectivityManager::class.java)?.unregisterNetworkCallback(callback) }
    }

    // ---------------------------------------------------------------- helpers

    private fun setState(next: Int, why: String) {
        if (state == next && reason == why && next != STATE_UNAVAILABLE) return
        state = next
        reason = why
        events.offer("state", stateEntry())
    }

    private fun stateEntry(): JSONObject = JSONObject()
        .put("k", KIND_STATE)
        .put("s", state)
        .put("r", reason)
        .put("i", socketId.orEmpty())
        .put("a", attempt)
        .put("w", if (retryAt > 0) (retryAt - now()).coerceAtLeast(0) else 0)

    private fun snapshot(): JSONObject = stateEntry()
        .put("e", lastError)
        .put("t", connectedAt)
        .put("q", events.pending())
        .put("ch", JSONArray().apply {
            channels.values.forEach { put(JSONObject().put("n", it.name).put("s", it.subscribed).put("p", it.authorizing || it.retry != null)) }
        })
        .put("j", JSONArray(journal.toList()))

    private fun record(message: String) {
        if (journal.size >= JOURNAL_SIZE) journal.removeLast()
        journal.addFirst(JSONObject().put("at", System.currentTimeMillis()).put("m", message))
    }

    private fun send(payload: JSONObject): Boolean = socket?.send(payload.toString()) == true

    private fun objectData(message: JSONObject): JSONObject = when (val data = message.opt("data")) {
        is JSONObject -> data
        is String -> runCatching { JSONObject(data) }.getOrDefault(JSONObject())
        else -> JSONObject()
    }

    private fun parseData(text: String): Any = runCatching {
        when (text.trimStart().firstOrNull()) {
            '{' -> JSONObject(text)
            '[' -> JSONArray(text)
            else -> text
        }
    }.getOrDefault(text)

    private fun post(block: () -> Unit) {
        if (executor.isShutdown) return
        runCatching { executor.execute { runCatching(block) } }
    }

    private fun now() = System.nanoTime() / 1_000_000

    companion object {
        const val STATE_CONNECTING = 1
        const val STATE_CONNECTED = 2
        const val STATE_UNAVAILABLE = 3
        const val STATE_DISCONNECTED = 4
        const val STATE_FAILED = 5

        const val KIND_STATE = 1
        const val KIND_MESSAGE = 2
        const val KIND_WHISPER = 3
        const val KIND_SUBSCRIBED = 4
        const val KIND_SUBSCRIPTION_FAILED = 5
        const val KIND_MEMBER_ADDED = 6
        const val KIND_MEMBER_REMOVED = 7

        private const val WHISPER_INTERVAL_MS = 100L
        private const val AUTH_RETRY_MIN_MS = 350L
        private const val AUTH_RETRY_MAX_MS = 8_000L
        private const val FORBIDDEN_RETRY_MS = 60_000L
        private const val JOURNAL_SIZE = 40
    }
}
