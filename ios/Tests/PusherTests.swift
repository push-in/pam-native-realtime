import Network
import PamNative
import XCTest
// Generated plugin target: PamPlugin<index>PushinbrPamNativeRealtime (index = plugin order).
@testable import PamPlugin0PushinbrPamNativeRealtime

/// XCTest mirror of PusherInstrumentedTest with a Network.framework WebSocket
/// server speaking the Pusher protocol. Uncompiled — needs Mac validation.
final class PusherTests: XCTestCase {
    private var server: PusherTestServer!
    private let module = RealtimeModule()

    override func setUpWithError() throws {
        server = try PusherTestServer()
        let ready = expectation(description: "server")
        server.onReady = { ready.fulfill() }
        server.start()
        wait(for: [ready], timeout: 5)
    }

    override func tearDown() {
        module.close()
        server.stop()
        super.tearDown()
    }

    private func call(_ method: String, _ values: [String: WireValue], timeout: TimeInterval = 10) -> [String: WireValue] {
        let done = expectation(description: method)
        var result: [String: WireValue] = [:]
        module.invoke(method: method, payload: (try? WireMap.encode(values)) ?? Data()) { _, payload in
            result = (try? WireMap.decode(payload)) ?? [:]
            done.fulfill()
        }
        wait(for: [done], timeout: timeout)
        return result
    }

    private func batch() -> [[String: Any]] {
        guard case let .text(json)? = call("pusherNext", ["client": .text("c1")])["batch"],
              let object = try? JSONSerialization.jsonObject(with: Data(json.utf8)) as? [String: Any] else { return [] }
        return object["events"] as? [[String: Any]] ?? []
    }

    private func until(_ predicate: ([String: Any]) -> Bool) -> [[String: Any]] {
        var seen: [[String: Any]] = []
        for _ in 0..<20 {
            let events = batch()
            seen += events
            if events.contains(where: predicate) { return seen }
        }
        XCTFail("event not received: \(seen)")
        return seen
    }

    private func connect() {
        let config = #"{"url":"ws://127.0.0.1:\#(server.port)/app/key","quietMs":30,"maxDelayMs":150,"backoffInitialMs":100,"backoffMaxMs":300}"#
        _ = call("pusherConnect", ["client": .text("c1"), "config": .text(config)])
        _ = until { ($0["k"] as? Int) == 1 && ($0["s"] as? Int) == 2 }
    }

    func testPublicChannelBurstArrivesAsOneBatch() {
        connect()
        _ = call("pusherSubscribe", ["client": .text("c1"), "channel": .text("news")])
        _ = until { ($0["k"] as? Int) == 4 && ($0["c"] as? String) == "news" }
        for index in 0..<20 { server.broadcast(event: "posted", channel: "news", data: #"{"n":\#(index)}"#) }
        Thread.sleep(forTimeInterval: 0.3)
        let events = batch().filter { ($0["k"] as? Int) == 2 }
        XCTAssertEqual(events.count, 20)
        XCTAssertEqual(events.first?["n"] as? String, "posted")
    }

    func testReconnectsWithBackoffAndResubscribes() {
        connect()
        _ = call("pusherSubscribe", ["client": .text("c1"), "channel": .text("news")])
        _ = until { ($0["k"] as? Int) == 4 }
        server.dropAll()
        _ = until { ($0["k"] as? Int) == 1 && ($0["s"] as? Int) == 3 }
        _ = until { ($0["k"] as? Int) == 4 && ($0["c"] as? String) == "news" }
        guard case let .text(status)? = call("pusherStatus", ["client": .text("c1")])["status"] else { return XCTFail("status") }
        XCTAssertTrue(status.contains("\"s\":2"))
    }

    func testPrivateChannelWithoutAuthEndpointReportsFailure() {
        connect()
        _ = call("pusherSubscribe", ["client": .text("c1"), "channel": .text("private-room")])
        let events = until { ($0["k"] as? Int) == 5 }
        XCTAssertEqual(events.last { ($0["k"] as? Int) == 5 }?["w"] as? Int, -1)
        XCTAssertEqual(call("pusherWhisper", ["client": .text("c1"), "channel": .text("private-room"), "event": .text("client-typing"), "data": .text("{}")])["sent"], .flag(false))
    }

    func testBatcherCoalescesKeyedEntriesAndReportsDrops() {
        let batcher = EventBatcher(queue: DispatchQueue(label: "t"), quietMillis: 5, maxDelayMillis: 20, capacity: 3, maxBatch: 10)
        batcher.offer("state", ["s": 1])
        batcher.offer("state", ["s": 2])
        batcher.offer(nil, ["m": 1])
        batcher.offer(nil, ["m": 2])
        batcher.offer(nil, ["m": 3])
        XCTAssertEqual(batcher.pending, 3)
        let done = expectation(description: "batch")
        batcher.next { _, payload in
            guard case let .text(json)? = (try? WireMap.decode(payload))?["batch"],
                  let object = try? JSONSerialization.jsonObject(with: Data(json.utf8)) as? [String: Any] else { return }
            XCTAssertEqual(object["dropped"] as? Int, 1)
            done.fulfill()
        }
        wait(for: [done], timeout: 2)
    }
}

/// Minimal Pusher-protocol server on Network.framework WebSockets.
final class PusherTestServer {
    private let listener: NWListener
    private var connections: [NWConnection] = []
    private let queue = DispatchQueue(label: "pusher.test.server")
    private var socketCounter = 0
    var onReady: (() -> Void)?

    var port: UInt16 { listener.port?.rawValue ?? 0 }

    init() throws {
        let parameters = NWParameters.tcp
        let options = NWProtocolWebSocket.Options()
        options.autoReplyPing = true
        parameters.defaultProtocolStack.applicationProtocols.insert(options, at: 0)
        listener = try NWListener(using: parameters, on: .any)
        listener.stateUpdateHandler = { [weak self] state in if case .ready = state { self?.onReady?() } }
        listener.newConnectionHandler = { [weak self] connection in self?.accept(connection) }
    }

    func start() { listener.start(queue: queue) }

    func stop() {
        queue.sync { connections.forEach { $0.cancel() } }
        listener.cancel()
    }

    func dropAll() { queue.sync { connections.forEach { $0.forceCancel() }; connections.removeAll() } }

    func broadcast(event: String, channel: String, data: String) {
        let frame = (try? JSONSerialization.data(withJSONObject: ["event": event, "channel": channel, "data": data])) ?? Data()
        queue.async { self.connections.forEach { self.send(frame, on: $0) } }
    }

    private func accept(_ connection: NWConnection) {
        connections.append(connection)
        connection.start(queue: queue)
        socketCounter += 1
        let established: [String: Any] = [
            "event": "pusher:connection_established",
            "data": #"{"socket_id":"\#(socketCounter).1","activity_timeout":120}"#,
        ]
        send((try? JSONSerialization.data(withJSONObject: established)) ?? Data(), on: connection)
        receive(on: connection)
    }

    private func receive(on connection: NWConnection) {
        connection.receiveMessage { [weak self] data, _, _, error in
            guard let self, error == nil else { return }
            if let data, let message = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
               message["event"] as? String == "pusher:subscribe",
               let channel = (message["data"] as? [String: Any])?["channel"] as? String {
                let reply: [String: Any] = ["event": "pusher_internal:subscription_succeeded", "channel": channel, "data": "{}"]
                self.send((try? JSONSerialization.data(withJSONObject: reply)) ?? Data(), on: connection)
            }
            self.receive(on: connection)
        }
    }

    private func send(_ data: Data, on connection: NWConnection) {
        let metadata = NWProtocolWebSocket.Metadata(opcode: .text)
        let context = NWConnection.ContentContext(identifier: "frame", metadata: [metadata])
        connection.send(content: data, contentContext: context, isComplete: true, completion: .idempotent)
    }
}
