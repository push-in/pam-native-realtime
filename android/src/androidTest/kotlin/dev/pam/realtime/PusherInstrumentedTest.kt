package dev.pam.realtime

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import dev.pam.nativeapp.modules.ModuleCompletion
import dev.pam.nativeapp.modules.ModuleResultStatus
import dev.pam.nativeapp.protocol.WireMap
import dev.pam.nativeapp.protocol.WireValue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.json.JSONArray
import org.json.JSONObject
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class PusherInstrumentedTest {
    private val context = InstrumentationRegistry.getInstrumentation().targetContext
    private val module = RealtimeModule(context)
    private val server = MockWebServer()
    private val sockets = CopyOnWriteArrayList<WebSocket>()
    private val inbound = CopyOnWriteArrayList<JSONObject>()
    private val authRequests = CopyOnWriteArrayList<RecordedRequest>()
    private val authBodies = CopyOnWriteArrayList<String>()
    private val connections = AtomicInteger()
    @Volatile private var authStatus = 200

    private data class Result(val ok: Boolean, val values: Map<String, WireValue>, val message: String)

    @Before
    fun start() {
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path?.startsWith("/app/test-key") == true -> MockResponse().withWebSocketUpgrade(PusherServer())
                request.path == "/broadcasting/auth" -> {
                    authRequests += request
                    val body = request.body.readUtf8()
                    authBodies += body
                    val channel = Regex("channel_name=([^&]+)").find(body)!!.groupValues[1].replace("%2D", "-")
                    val auth = JSONObject().put("auth", "test-key:signature")
                    if (channel.startsWith("presence")) auth.put("channel_data", JSONObject().put("user_id", "7").put("user_info", JSONObject().put("name", "Ana")).toString())
                    MockResponse().setResponseCode(authStatus).setBody(if (authStatus == 200) auth.toString() else "{}")
                }
                else -> MockResponse().setResponseCode(404)
            }
        }
        server.start()
    }

    @After
    fun stop() {
        module.close()
        sockets.forEach { runCatching { it.close(1001, null) } }
        Thread.sleep(300)
        // MockWebServer may still wait on a half-closed upgrade; teardown must not mask results.
        runCatching { server.shutdown() }
    }

    private inner class PusherServer : WebSocketListener() {
        override fun onOpen(webSocket: WebSocket, response: Response) {
            sockets += webSocket
            val id = connections.incrementAndGet()
            webSocket.send(frame("pusher:connection_established", null, JSONObject().put("socket_id", "$id.$id").put("activity_timeout", 30).toString()))
        }

        override fun onMessage(webSocket: WebSocket, text: String) {
            val message = JSONObject(text)
            inbound += message
            if (message.getString("event") == "pusher:subscribe") {
                val channel = message.getJSONObject("data").getString("channel")
                val data = if (channel.startsWith("presence-")) {
                    JSONObject().put("presence", JSONObject().put("ids", JSONArray().put("7").put("9")).put("hash", JSONObject().put("7", JSONObject().put("name", "Ana")).put("9", JSONObject().put("name", "Bia"))).put("count", 2)).toString()
                } else "{}"
                webSocket.send(frame("pusher_internal:subscription_succeeded", channel, data))
            }
            if (message.getString("event") == "pusher:ping") webSocket.send(frame("pusher:pong", null, "{}"))
        }
    }

    private fun frame(event: String, channel: String?, data: String, user: String? = null) =
        JSONObject().put("event", event).put("data", data).apply {
            channel?.let { put("channel", it) }
            user?.let { put("user_id", it) }
        }.toString()

    private fun call(method: String, values: Map<String, WireValue>, timeout: Long = 10): Result {
        val latch = CountDownLatch(1)
        val result = AtomicReference<Result>()
        module.invoke(method, WireMap.encode(values), ModuleCompletion { status, payload ->
            result.set(if (status == ModuleResultStatus.SUCCESS) Result(true, WireMap.decode(payload), "") else Result(false, emptyMap(), String(payload)))
            latch.countDown()
        })
        assertTrue("$method timed out", latch.await(timeout, TimeUnit.SECONDS))
        return result.get()
    }

    private fun pusher(method: String, vararg values: Pair<String, WireValue>) =
        call(method, mapOf("client" to WireValue.Text("default"), *values))

    private fun connect(extra: JSONObject.() -> Unit = {}) {
        val config = JSONObject()
            .put("url", server.url("/app/test-key").toString().replace("http://", "ws://"))
            .put("authEndpoint", server.url("/broadcasting/auth").toString())
            .put("bearer", "token-1")
            .put("backoffInitialMs", 100)
            .put("backoffMaxMs", 200)
            .apply(extra)
        assertTrue(pusher("pusherConnect", "config" to WireValue.Text(config.toString())).ok)
    }

    private fun batch(): Pair<List<JSONObject>, Int> {
        val result = pusher("pusherNext")
        assertTrue(result.message, result.ok)
        val payload = JSONObject((result.values["batch"] as WireValue.Text).value)
        val events = payload.getJSONArray("events")
        return (0 until events.length()).map { events.getJSONObject(it) } to payload.getInt("dropped")
    }

    /** Reads batches until [predicate] matches an event; returns every event read. */
    private fun until(predicate: (JSONObject) -> Boolean): List<JSONObject> {
        val all = ArrayList<JSONObject>()
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            val (events, _) = batch()
            all += events
            if (events.any(predicate)) return all
        }
        throw AssertionError("Expected event not delivered: $all")
    }

    private fun connected(event: JSONObject) = event.getInt("k") == 1 && event.getInt("s") == 2
    private fun subscribed(channel: String) = { e: JSONObject -> e.getInt("k") == 4 && e.getString("c") == channel }
    private fun waitFor(condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + 5_000
        while (!condition() && System.currentTimeMillis() < deadline) Thread.sleep(20)
        assertTrue(condition())
    }

    @Test
    fun authorizesPrivateChannelsAndDeliversABurstAsOneBatch() {
        connect()
        pusher("pusherSubscribe", "channel" to WireValue.Text("private-chat.42"))
        until(subscribed("private-chat.42"))
        val auth = authRequests.single()
        assertEquals("Bearer token-1", auth.getHeader("Authorization"))
        assertTrue(authBodies.single().contains("socket_id=1.1"))

        val socket = sockets.single()
        Thread {
            Thread.sleep(150)
            repeat(50) { socket.send(frame("message.sent", "private-chat.42", JSONObject().put("n", it).toString())) }
        }.start()
        val (events, dropped) = batch()
        assertEquals(0, dropped)
        assertEquals(50, events.count { it.getInt("k") == 2 && it.getString("n") == "message.sent" })
        assertEquals((0 until 50).toList(), events.map { JSONObject(it.getString("d")).getInt("n") })
    }

    @Test
    fun coalescesWhisperBurstsInBothDirections() {
        connect()
        pusher("pusherSubscribe", "channel" to WireValue.Text("presence-chat-typing.42"))
        val ready = until(subscribed("presence-chat-typing.42"))
        val members = JSONObject(ready.last(subscribed("presence-chat-typing.42")).getString("m"))
        assertEquals("Bia", members.getJSONObject("9").getString("name"))

        repeat(5) { pusher("pusherWhisper", "channel" to WireValue.Text("presence-chat-typing.42"), "event" to WireValue.Text("client-typing"), "data" to WireValue.Text("""{"n":$it}""")) }
        waitFor { inbound.count { it.getString("event") == "client-typing" } == 2 }
        Thread.sleep(250)
        val sent = inbound.filter { it.getString("event") == "client-typing" }
        assertEquals(2, sent.size)
        assertEquals(4, sent.last().getJSONObject("data").getInt("n"))

        val socket = sockets.single()
        Thread {
            Thread.sleep(100)
            repeat(10) { socket.send(frame("client-typing", "presence-chat-typing.42", """{"n":$it}""", "9")) }
            socket.send(frame("pusher_internal:member_added", "presence-chat-typing.42", """{"user_id":"11","user_info":{"name":"Caio"}}"""))
            socket.send(frame("pusher_internal:member_removed", "presence-chat-typing.42", """{"user_id":"9"}"""))
        }.start()
        val events = until { it.getInt("k") == 7 }
        val whispers = events.filter { it.getInt("k") == 3 }
        assertEquals(1, whispers.size)
        assertEquals("9", whispers.single().getString("u"))
        assertEquals(9, JSONObject(whispers.single().getString("d")).getInt("n"))
        assertEquals("11", events.single { it.getInt("k") == 6 }.getString("u"))
    }

    @Test
    fun reconnectsWithBackoffAndResubscribes() {
        connect()
        pusher("pusherSubscribe", "channel" to WireValue.Text("private-chat.42"))
        pusher("pusherSubscribe", "channel" to WireValue.Text("public-feed"))
        until { e -> e.getInt("k") == 4 && e.getString("c") == "public-feed" || subscribed("private-chat.42")(e) }
        waitFor { inbound.count { it.getString("event") == "pusher:subscribe" } == 2 }

        sockets.single().close(1001, "going away")
        until { connected(it) && it.getString("i") == "2.2" }
        waitFor { inbound.count { it.getString("event") == "pusher:subscribe" } == 4 }
        assertEquals(2, authRequests.size)

        val status = JSONObject((pusher("pusherStatus").values["status"] as WireValue.Text).value)
        assertEquals(2, status.getInt("s"))
        assertEquals("2.2", status.getString("i"))
        assertEquals(2, status.getJSONArray("ch").length())
    }

    @Test
    fun terminalServerErrorsWaitForTheTerminalRetryWindow() {
        connect { put("terminalRetryMs", 60_000) }
        until(::connected)
        sockets.single().send(frame("pusher:error", null, """{"code":4001,"message":"App disabled"}"""))
        val failed = until { it.getInt("k") == 1 && it.getInt("s") == 5 }.last { it.getInt("k") == 1 }
        assertTrue(failed.getLong("w") > 50_000)
        assertTrue(failed.getString("r").contains("4001"))
    }

    @Test
    fun reportsForbiddenChannelAuthWithASlowRetry() {
        authStatus = 403
        connect()
        pusher("pusherSubscribe", "channel" to WireValue.Text("private-chat.99"))
        val failure = until { it.getInt("k") == 5 }.last { it.getInt("k") == 5 }
        assertEquals(403, failure.getInt("h"))
        assertEquals(60_000L, failure.getLong("w"))

        authStatus = 200
        pusher("pusherToken", "bearer" to WireValue.Text("token-2"))
        until(subscribed("private-chat.99"))
        assertEquals("Bearer token-2", authRequests.last().getHeader("Authorization"))
    }

    @Test
    fun disconnectFailsThePendingReadAndForgetsTheClient() {
        connect()
        until(::connected)
        val latch = CountDownLatch(1)
        val failed = AtomicReference<String>()
        module.invoke("pusherNext", WireMap.encode(mapOf("client" to WireValue.Text("default"))), ModuleCompletion { status, payload ->
            if (status == ModuleResultStatus.FAILURE) failed.set(String(payload))
            latch.countDown()
        })
        assertTrue(pusher("pusherDisconnect").ok)
        assertTrue(latch.await(5, TimeUnit.SECONDS))
        assertEquals("Realtime client closed", failed.get())
        assertTrue(!pusher("pusherStatus").ok)
    }
}
