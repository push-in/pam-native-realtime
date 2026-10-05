import Foundation
import PamNative

/// Push-style, coalescing event channel (Android EventBatcher parity).
///
/// PHP keeps exactly one pending `pusherNext` completion. Events are buffered
/// and released as ONE batch once the stream has been quiet for [quietMillis]
/// (or [maxDelayMillis] after the first buffered event during a burst).
/// Entries with a coalescing key replace their previous occurrence.
final class EventBatcher: @unchecked Sendable {
    struct Entry {
        let key: String?
        let json: [String: Any]
    }

    private let queue: DispatchQueue
    private let quietMillis: Int64
    private let maxDelayMillis: Int64
    private let capacity: Int
    private let maxBatch: Int
    private let lock = NSLock()
    private var entries: [Entry] = []
    private var waiter: ModuleCompletion?
    private var flush: DispatchWorkItem?
    private var firstAt: Int64 = 0
    private var lastAt: Int64 = 0
    private var dropped = 0
    private var closed = false
    var clock: () -> Int64 = { Int64(DispatchTime.now().uptimeNanoseconds / 1_000_000) }

    init(queue: DispatchQueue, quietMillis: Int64, maxDelayMillis: Int64, capacity: Int, maxBatch: Int) {
        self.queue = queue
        self.quietMillis = quietMillis
        self.maxDelayMillis = maxDelayMillis
        self.capacity = capacity
        self.maxBatch = maxBatch
    }

    func offer(_ key: String?, _ json: [String: Any]) {
        lock.lock()
        defer { lock.unlock() }
        guard !closed else { return }
        if let key, let index = entries.firstIndex(where: { $0.key == key }) {
            entries.remove(at: index)
        }
        if entries.count >= capacity {
            entries.remove(at: entries.firstIndex { $0.key != nil } ?? 0)
            dropped += 1
        }
        entries.append(Entry(key: key, json: json))
        let now = clock()
        if firstAt == 0 { firstAt = now }
        lastAt = now
        scheduleLocked(now)
    }

    func next(_ completion: @escaping ModuleCompletion) {
        lock.lock()
        if closed {
            lock.unlock()
            completion(.failure, Data("Realtime client closed".utf8))
            return
        }
        let replaced = waiter
        waiter = completion
        scheduleLocked(clock())
        lock.unlock()
        replaced?(.failure, Data("Event read replaced".utf8))
    }

    func close() {
        lock.lock()
        guard !closed else {
            lock.unlock()
            return
        }
        closed = true
        entries.removeAll()
        flush?.cancel()
        flush = nil
        let pending = waiter
        waiter = nil
        lock.unlock()
        pending?(.failure, Data("Realtime client closed".utf8))
    }

    var pending: Int {
        lock.lock()
        defer { lock.unlock() }
        return entries.count
    }

    private func scheduleLocked(_ now: Int64) {
        guard waiter != nil, !entries.isEmpty, flush == nil else { return }
        let work = DispatchWorkItem { [weak self] in self?.release() }
        flush = work
        queue.asyncAfter(deadline: .now() + .milliseconds(Int(dueIn(now))), execute: work)
    }

    func dueIn(_ now: Int64) -> Int64 {
        max(min(lastAt + quietMillis - now, firstAt + maxDelayMillis - now), 0)
    }

    func release() {
        lock.lock()
        flush = nil
        guard let receiver = waiter, !entries.isEmpty, !closed else {
            lock.unlock()
            return
        }
        let delay = dueIn(clock())
        if delay > 0 {
            let work = DispatchWorkItem { [weak self] in self?.release() }
            flush = work
            queue.asyncAfter(deadline: .now() + .milliseconds(Int(delay)), execute: work)
            lock.unlock()
            return
        }
        waiter = nil
        let count = min(maxBatch, entries.count)
        let batch = entries.prefix(count).map(\.json)
        entries.removeFirst(count)
        // Leftovers are already late: the next read releases them at once.
        if entries.isEmpty {
            firstAt = 0
            lastAt = 0
        }
        let payload: [String: Any] = ["events": batch, "dropped": dropped]
        dropped = 0
        lock.unlock()
        let data = (try? JSONSerialization.data(withJSONObject: payload)).map { String(decoding: $0, as: UTF8.self) } ?? "{\"events\":[]}"
        receiver(.success, (try? WireMap.encode(["batch": .text(data)])) ?? Data())
    }
}
