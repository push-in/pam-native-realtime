package dev.pam.realtime

import dev.pam.nativeapp.modules.ModuleCompletion
import dev.pam.nativeapp.protocol.WireValue
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import org.json.JSONArray
import org.json.JSONObject

/**
 * Push-style, coalescing event channel.
 *
 * PHP keeps exactly one pending `pusherNext` completion. Events are buffered
 * natively and released as ONE batch once the stream has been quiet for
 * [quietMillis] (or [maxDelayMillis] after the first buffered event during a
 * sustained burst), so a burst of frames costs a single PHP callback and a
 * single render. Entries that carry a coalescing key (connection state,
 * whispers per sender, channel errors) replace their previous occurrence.
 */
internal class EventBatcher(
    private val scheduler: ScheduledExecutorService,
    private val quietMillis: Long,
    private val maxDelayMillis: Long,
    private val capacity: Int,
    private val maxBatch: Int,
) {
    class Entry(val key: String?, val json: JSONObject)

    private val entries = ArrayList<Entry>()
    private var waiter: ModuleCompletion? = null
    private var flush: ScheduledFuture<*>? = null
    private var firstAt = 0L
    private var lastAt = 0L
    private var dropped = 0
    private var closed = false

    fun offer(key: String?, json: JSONObject) {
        synchronized(this) {
            if (closed) return
            if (key != null) {
                val index = entries.indexOfFirst { it.key == key }
                if (index >= 0) entries.removeAt(index)
            }
            if (entries.size >= capacity) {
                val victim = entries.indexOfFirst { it.key != null }.takeIf { it >= 0 } ?: 0
                entries.removeAt(victim)
                dropped++
            }
            entries.add(Entry(key, json))
            val now = now()
            if (firstAt == 0L) firstAt = now
            lastAt = now
            scheduleLocked(now)
        }
    }

    fun next(completion: ModuleCompletion) {
        val replaced: ModuleCompletion?
        synchronized(this) {
            if (closed) {
                replaced = null
            } else {
                replaced = waiter
                waiter = completion
                scheduleLocked(now())
            }
        }
        replaced?.failure("Event read replaced")
        if (isClosed()) completion.failure("Realtime client closed")
    }

    fun close() {
        val pending = synchronized(this) {
            if (closed) return
            closed = true
            entries.clear()
            flush?.cancel(false)
            flush = null
            waiter.also { waiter = null }
        }
        pending?.failure("Realtime client closed")
    }

    @Synchronized
    fun pending(): Int = entries.size

    @Synchronized
    private fun isClosed() = closed

    private fun scheduleLocked(now: Long) {
        if (waiter == null || entries.isEmpty() || flush != null) return
        flush = scheduler.schedule({ release() }, dueIn(now), TimeUnit.MILLISECONDS)
    }

    private fun dueIn(now: Long): Long {
        val quiet = lastAt + quietMillis - now
        val ceiling = firstAt + maxDelayMillis - now
        return minOf(quiet, ceiling).coerceAtLeast(0)
    }

    private fun release() {
        val completion: ModuleCompletion
        val payload: String
        synchronized(this) {
            flush = null
            val receiver = waiter ?: return
            if (entries.isEmpty() || closed) return
            val now = now()
            val delay = dueIn(now)
            if (delay > 0) {
                flush = scheduler.schedule({ release() }, delay, TimeUnit.MILLISECONDS)
                return
            }
            waiter = null
            val count = minOf(maxBatch, entries.size)
            val events = JSONArray()
            repeat(count) { events.put(entries[it].json) }
            entries.subList(0, count).clear()
            // Leftovers are already late: keep the original timestamps so the
            // next read releases them immediately.
            if (entries.isEmpty()) {
                firstAt = 0L
                lastAt = 0L
            }
            payload = JSONObject().put("events", events).put("dropped", dropped).toString()
            dropped = 0
            completion = receiver
        }
        completion.success(mapOf("batch" to WireValue.Text(payload)))
    }

    private fun now() = System.nanoTime() / 1_000_000
}
