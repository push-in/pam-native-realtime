import Foundation
import Network
import PamNative

/// Configuration decoded from the PHP `PusherConnector`.
final class PusherConfig: @unchecked Sendable {
    let url: URL
    let headers: [String: String]
    let authEndpoint: URL?
    let authHeaders: [String: String]
    var bearer: String?
    let backoffInitialMs: Int64
    let backoffMaxMs: Int64
    let backoffMultiplier: Double
    let activityTimeoutMs: Int64
    let pongTimeoutMs: Int64
    let handshakeTimeoutMs: Int64
    let terminalRetryMs: Int64
    let quietMs: Int64
    let maxDelayMs: Int64
    let capacity: Int
    let maxBatch: Int

    init(_ json: String) throws {
        guard let object = try JSONSerialization.jsonObject(with: Data(json.utf8)) as? [String: Any],
              let text = object["url"] as? String, text.hasPrefix("wss://") || text.hasPrefix("ws://"),
              let url = URL(string: text) else {
            throw RealtimeFailure("Pusher URL must use wss://")
        }
        func long(_ key: String, _ fallback: Int64, _ range: ClosedRange<Int64>) -> Int64 {
            min(max((object[key] as? NSNumber)?.int64Value ?? fallback, range.lowerBound), range.upperBound)
        }
        func map(_ key: String) -> [String: String] {
            ((object[key] as? [String: Any]) ?? [:]).compactMapValues { $0 as? String }
        }
        self.url = url
        headers = map("headers")
        authEndpoint = (object["authEndpoint"] as? String).flatMap { $0.isEmpty ? nil : URL(string: $0) }
        authHeaders = map("authHeaders")
        bearer = (object["bearer"] as? String).flatMap { $0.isEmpty ? nil : $0 }
        backoffInitialMs = long("backoffInitialMs", 400, 50...60_000)
        backoffMaxMs = long("backoffMaxMs", 15_000, 100...600_000)
        backoffMultiplier = min(max((object["backoffMultiplier"] as? NSNumber)?.doubleValue ?? 1.8, 1), 10)
        activityTimeoutMs = long("activityTimeoutMs", 30_000, 1_000...600_000)
        pongTimeoutMs = long("pongTimeoutMs", 12_000, 500...120_000)
        handshakeTimeoutMs = long("handshakeTimeoutMs", 10_000, 1_000...120_000)
        terminalRetryMs = long("terminalRetryMs", 300_000, 1_000...86_400_000)
        quietMs = long("quietMs", 32, 0...1_000)
        maxDelayMs = long("maxDelayMs", 150, 0...5_000)
        capacity = Int(long("capacity", 2_048, 16...65_536))
        maxBatch = Int(long("maxBatch", 512, 1...8_192))
    }
}

struct RealtimeFailure: LocalizedError {
    let message: String
    init(_ message: String) { self.message = message }
    var errorDescription: String? { message }
}

/// One Pusher-protocol (Pusher Channels / Laravel Reverb / Soketi) connection
/// over URLSessionWebSocketTask. All state lives on a private serial queue;
/// every observable change is offered to [events] (coalesced batches).
final class PusherClient: NSObject, URLSessionWebSocketDelegate, @unchecked Sendable {
    static let stateConnecting = 1
    static let stateConnected = 2
    static let stateUnavailable = 3
    static let stateDisconnected = 4
    static let stateFailed = 5
    static let kindState = 1
    static let kindMessage = 2
    static let kindWhisper = 3
    static let kindSubscribed = 4
    static let kindSubscriptionFailed = 5
    static let kindMemberAdded = 6
    static let kindMemberRemoved = 7
    private static let whisperIntervalMs: Int64 = 100
    private static let authRetryMinMs: Int64 = 350
    private static let authRetryMaxMs: Int64 = 8_000
    private static let forbiddenRetryMs: Int64 = 60_000
    private static let journalSize = 40

    private final class ChannelState {
        let name: String
        var subscribed = false
        var authorizing = false
        var authAttempt = 0
        var retry: DispatchWorkItem?
        var presence: Bool { name.hasPrefix("presence-") }
        var requiresAuth: Bool { name.hasPrefix("private-") || presence }

        init(_ name: String) { self.name = name }
    }

    private final class PendingWhisper {
        var data: Any
        var task: DispatchWorkItem?

        init(_ data: Any) { self.data = data }
    }

    let id: String
    private let config: PusherConfig
    private let queue: DispatchQueue
    let events: EventBatcher
    private var session: URLSession?
    private var socket: URLSessionWebSocketTask?
    private var channels: [String: ChannelState] = [:]
    private var channelOrder: [String] = []
    private var lastWhisperAt: [String: Int64] = [:]
    private var pendingWhispers: [String: PendingWhisper] = [:]
    private var journal: [[String: Any]] = []
    private var generation: Int64 = 0
    private(set) var state = stateDisconnected
    private var reason = ""
    private var socketId: String?
    private var attempt = 0
    private var retryAt: Int64 = 0
    private var retryNotBefore: Int64 = 0
    private var activityTimeoutMs: Int64
    private var lastFrameAt: Int64 = 0
    private var pingSentAt: Int64 = 0
    private var connectedAt: Int64 = 0
    private var lastError = ""
    private var reconnectTask: DispatchWorkItem?
    private var handshakeTask: DispatchWorkItem?
    private var liveness: DispatchSourceTimer?
    private var monitor: NWPathMonitor?
    private var knownPath: NWPath.Status?
    private var closed = false

    init(id: String, config: PusherConfig) {
        self.id = id
        self.config = config
        queue = DispatchQueue(label: "pam-realtime-\(id)")
        events = EventBatcher(
            queue: queue,
            quietMillis: config.quietMs,
            maxDelayMillis: max(config.quietMs, config.maxDelayMs),
            capacity: config.capacity,
            maxBatch: config.maxBatch
        )
        activityTimeoutMs = config.activityTimeoutMs
        super.init()
    }

    // MARK: API

    func start() {
        queue.async {
            self.startNetworkMonitor()
            self.openSocket("connect")
        }
    }

    func subscribe(_ name: String) {
        queue.async {
            guard self.channels[name] == nil else { return }
            let channel = ChannelState(name)
            self.channels[name] = channel
            self.channelOrder.append(name)
            self.subscribeChannel(channel)
        }
    }

    func unsubscribe(_ name: String) {
        queue.async {
            guard let channel = self.channels.removeValue(forKey: name) else { return }
            self.channelOrder.removeAll { $0 == name }
            channel.retry?.cancel()
            for key in self.pendingWhispers.keys where key.hasPrefix("\(name)\u{0}") {
                self.pendingWhispers.removeValue(forKey: key)?.task?.cancel()
            }
            if channel.subscribed || channel.authorizing {
                self.send(["event": "pusher:unsubscribe", "data": ["channel": name]])
            }
            self.record("Left \(name)")
        }
    }

    func whisper(_ channelName: String, event: String, data: String, completion: @escaping ModuleCompletion) {
        queue.async {
            guard let channel = self.channels[channelName], channel.subscribed, channel.requiresAuth,
                  self.state == Self.stateConnected else {
                completion(.success, (try? WireMap.encode(["sent": .flag(false)])) ?? Data())
                return
            }
            let payload = Self.parseData(data)
            let key = "\(channelName)\u{0}\(event)"
            let now = self.now()
            let elapsed = now - (self.lastWhisperAt[key] ?? 0)
            if let pending = self.pendingWhispers[key] {
                // Trailing-edge throttle: only the newest payload of a burst is sent.
                pending.data = payload
            } else if elapsed >= Self.whisperIntervalMs {
                self.lastWhisperAt[key] = now
                self.send(["event": event, "channel": channelName, "data": payload])
            } else {
                let entry = PendingWhisper(payload)
                self.pendingWhispers[key] = entry
                let task = DispatchWorkItem { [weak self] in
                    guard let self else { return }
                    self.pendingWhispers[key] = nil
                    if !self.closed, self.channels[channelName]?.subscribed == true, self.state == Self.stateConnected {
                        self.lastWhisperAt[key] = self.now()
                        self.send(["event": event, "channel": channelName, "data": entry.data])
                    }
                }
                entry.task = task
                self.queue.asyncAfter(deadline: .now() + .milliseconds(Int(Self.whisperIntervalMs - elapsed)), execute: task)
            }
            completion(.success, (try? WireMap.encode(["sent": .flag(true)])) ?? Data())
        }
    }

    func token(_ bearer: String?) {
        queue.async {
            self.config.bearer = bearer
            // A refreshed credential usually fixes 401/403 channel auth: retry now.
            for channel in self.orderedChannels() where !channel.subscribed && !channel.authorizing && channel.retry != nil {
                channel.retry?.cancel()
                channel.retry = nil
                channel.authAttempt = 0
                self.subscribeChannel(channel)
            }
        }
    }

    func reconnect() {
        queue.async {
            self.attempt = 0
            self.retryNotBefore = 0
            self.closeSocket(1000, "reconnect")
            self.openSocket("reconnect")
        }
    }

    func status(_ completion: @escaping ModuleCompletion) {
        queue.async {
            let data = (try? JSONSerialization.data(withJSONObject: self.snapshot())).map { String(decoding: $0, as: UTF8.self) } ?? "{}"
            completion(.success, (try? WireMap.encode(["status": .text(data)])) ?? Data())
        }
    }

    func close() {
        queue.async {
            guard !self.closed else { return }
            self.closed = true
            self.monitor?.cancel()
            self.monitor = nil
            self.reconnectTask?.cancel()
            self.pendingWhispers.values.forEach { $0.task?.cancel() }
            self.pendingWhispers.removeAll()
            self.channels.values.forEach { $0.retry?.cancel() }
            self.channels.removeAll()
            self.channelOrder.removeAll()
            self.closeSocket(1000, "disconnect")
            self.state = Self.stateDisconnected
            self.events.close()
            self.session?.invalidateAndCancel()
            self.session = nil
        }
    }

    // MARK: Socket

    private func openSocket(_ cause: String) {
        guard !closed else { return }
        reconnectTask?.cancel()
        reconnectTask = nil
        retryAt = 0
        generation += 1
        let current = generation
        socketId = nil
        setState(Self.stateConnecting, cause)
        record("Connecting (\(cause))")
        if session == nil {
            let configuration = URLSessionConfiguration.ephemeral
            configuration.timeoutIntervalForRequest = Double(config.handshakeTimeoutMs) / 1_000
            session = URLSession(configuration: configuration, delegate: self, delegateQueue: nil)
        }
        var request = URLRequest(url: config.url)
        config.headers.forEach { request.setValue($1, forHTTPHeaderField: $0) }
        let task = session!.webSocketTask(with: request)
        task.maximumMessageSize = 16 * 1_024 * 1_024
        task.taskDescription = String(current)
        socket = task
        task.resume()
        receive(task, owner: current)
        handshakeTask?.cancel()
        let handshake = DispatchWorkItem { [weak self] in
            guard let self, current == self.generation, self.state == Self.stateConnecting else { return }
            self.lost(current, "Handshake timed out")
        }
        handshakeTask = handshake
        queue.asyncAfter(deadline: .now() + .milliseconds(Int(config.handshakeTimeoutMs)), execute: handshake)
    }

    private func receive(_ task: URLSessionWebSocketTask, owner: Int64) {
        task.receive { [weak self] result in
            guard let self else { return }
            self.queue.async {
                guard owner == self.generation else { return }
                switch result {
                case let .success(.string(text)):
                    self.frame(text)
                    self.receive(task, owner: owner)
                case let .success(.data(data)):
                    self.frame(String(decoding: data, as: UTF8.self))
                    self.receive(task, owner: owner)
                case .success:
                    self.receive(task, owner: owner)
                case let .failure(error):
                    self.lost(owner, error.localizedDescription)
                }
            }
        }
    }

    func urlSession(
        _ session: URLSession,
        webSocketTask: URLSessionWebSocketTask,
        didCloseWith closeCode: URLSessionWebSocketTask.CloseCode,
        reason: Data?
    ) {
        let owner = Int64(webSocketTask.taskDescription ?? "") ?? -1
        let text = reason.map { String(decoding: $0, as: UTF8.self) } ?? ""
        queue.async {
            guard owner == self.generation else { return }
            self.lost(owner, text.isEmpty ? "Closed (\(closeCode.rawValue))" : text, code: closeCode.rawValue)
        }
    }

    private func closeSocket(_ code: Int, _ why: String) {
        generation += 1
        handshakeTask?.cancel()
        liveness?.cancel()
        liveness = nil
        socket?.cancel(with: URLSessionWebSocketTask.CloseCode(rawValue: code) ?? .normalClosure, reason: Data(why.utf8))
        socket = nil
        socketId = nil
        pingSentAt = 0
        resetChannels()
    }

    /// The current socket is gone: forget it and schedule a reconnect.
    private func lost(_ owner: Int64, _ why: String, code: Int = 0, immediate: Bool = false) {
        guard owner == generation, !closed else { return }
        lastError = why
        record("Connection lost: \(why)")
        closeSocket(1000, "lost")
        scheduleReconnect(why, immediate: immediate || (4200...4299).contains(code))
    }

    private func scheduleReconnect(_ why: String, immediate: Bool) {
        guard !closed else { return }
        reconnectTask?.cancel()
        let now = now()
        let terminal = retryNotBefore - now
        let delay: Int64
        if terminal > 0 {
            delay = terminal
            retryAt = now + delay
            setState(Self.stateFailed, why)
        } else {
            attempt += 1
            delay = immediate ? 0 : Self.backoff(attempt, floor: config.backoffInitialMs, ceiling: config.backoffMaxMs, multiplier: config.backoffMultiplier)
            retryAt = now + delay
            setState(Self.stateUnavailable, why)
        }
        let task = DispatchWorkItem { [weak self] in
            guard let self else { return }
            self.reconnectTask = nil
            self.openSocket("retry")
        }
        reconnectTask = task
        queue.asyncAfter(deadline: .now() + .milliseconds(Int(delay)), execute: task)
    }

    /// Full-jitter exponential backoff between [floor] and the growing cap.
    static func backoff(_ attempt: Int, floor: Int64, ceiling: Int64, multiplier: Double) -> Int64 {
        let cap = max(Int64(min(Double(ceiling), Double(floor) * pow(multiplier, Double(min(attempt - 1, 30))))), floor)
        return cap <= floor ? floor : Int64.random(in: floor...cap)
    }

    // MARK: Frames

    private func frame(_ text: String) {
        lastFrameAt = now()
        pingSentAt = 0
        guard let message = (try? JSONSerialization.jsonObject(with: Data(text.utf8))) as? [String: Any] else {
            record("Ignored a malformed frame")
            return
        }
        let event = message["event"] as? String ?? ""
        let channel = message["channel"] as? String
        switch event {
        case "pusher:connection_established":
            established(Self.objectData(message))
        case "pusher:error":
            serverError(Self.objectData(message))
        case "pusher:ping":
            send(["event": "pusher:pong", "data": [String: Any]()])
        case "pusher:pong":
            break
        case "pusher_internal:subscription_succeeded":
            if let channel { subscribed(channel, Self.objectData(message)) }
        case "pusher:subscription_error":
            if let channel, let state = channels[channel] {
                let data = Self.objectData(message)
                failed(state, status: (data["status"] as? NSNumber)?.intValue ?? 0, message: data["error"] as? String ?? "Subscription rejected")
            }
        case "pusher_internal:member_added":
            if let channel, channels[channel]?.subscribed == true {
                let data = Self.objectData(message)
                events.offer(nil, [
                    "k": Self.kindMemberAdded, "c": channel, "u": Self.string(data["user_id"]),
                    "d": data["user_info"].map(Self.jsonString) ?? "null",
                ])
            }
        case "pusher_internal:member_removed":
            if let channel, channels[channel]?.subscribed == true {
                let data = Self.objectData(message)
                events.offer(nil, ["k": Self.kindMemberRemoved, "c": channel, "u": Self.string(data["user_id"])])
            }
        default:
            guard let channel, !event.isEmpty, !event.hasPrefix("pusher"), channels[channel] != nil else { return }
            let raw: String
            switch message["data"] {
            case nil, is NSNull: raw = ""
            case let text as String: raw = text
            case let other?: raw = Self.jsonString(other)
            }
            let user = Self.string(message["user_id"])
            let whisper = event.hasPrefix("client-")
            events.offer(
                whisper ? "w\u{0}\(channel)\u{0}\(event)\u{0}\(user)" : nil,
                ["k": whisper ? Self.kindWhisper : Self.kindMessage, "c": channel, "n": event, "d": raw, "u": user]
            )
        }
    }

    private func established(_ data: [String: Any]) {
        guard let id = data["socket_id"] as? String, !id.isEmpty else {
            lost(generation, "Handshake without socket id")
            return
        }
        handshakeTask?.cancel()
        socketId = id
        let serverActivity = ((data["activity_timeout"] as? NSNumber)?.int64Value ?? 0) * 1_000
        activityTimeoutMs = serverActivity > 0 ? min(serverActivity, config.activityTimeoutMs) : config.activityTimeoutMs
        attempt = 0
        retryAt = 0
        retryNotBefore = 0
        lastError = ""
        connectedAt = Int64(Date().timeIntervalSince1970 * 1_000)
        record("Connected as \(id)")
        setState(Self.stateConnected, "")
        startLiveness()
        orderedChannels().forEach(subscribeChannel)
    }

    private func serverError(_ data: [String: Any]) {
        let code = (data["code"] as? NSNumber)?.intValue ?? 0
        let message = (data["message"] as? String).flatMap { $0.isEmpty ? nil : $0 } ?? "Pusher error"
        lastError = code > 0 ? "\(message) (\(code))" : message
        record("Server error: \(lastError)")
        switch code {
        case 4000...4099:
            retryNotBefore = now() + config.terminalRetryMs
            lost(generation, lastError, code: code)
        case 4100...4299:
            lost(generation, lastError, code: code)
        default:
            var entry = stateEntry()
            entry["r"] = lastError
            events.offer("error", entry)
        }
    }

    private func subscribed(_ name: String, _ data: [String: Any]) {
        guard let channel = channels[name] else { return }
        channel.subscribed = true
        channel.authorizing = false
        channel.authAttempt = 0
        channel.retry?.cancel()
        channel.retry = nil
        record("Subscribed \(name)")
        var entry: [String: Any] = ["k": Self.kindSubscribed, "c": name]
        if channel.presence {
            let presence = data["presence"] as? [String: Any] ?? [:]
            entry["m"] = Self.jsonString(presence["hash"] as? [String: Any] ?? [:])
        }
        events.offer("s\u{0}\(name)", entry)
    }

    // MARK: Channels

    private func orderedChannels() -> [ChannelState] { channelOrder.compactMap { channels[$0] } }

    private func resetChannels() {
        for channel in channels.values {
            channel.subscribed = false
            channel.authorizing = false
            channel.retry?.cancel()
            channel.retry = nil
        }
    }

    private func subscribeChannel(_ channel: ChannelState) {
        guard !closed, state == Self.stateConnected, let sid = socketId, !channel.subscribed, !channel.authorizing else { return }
        guard channel.requiresAuth else {
            send(["event": "pusher:subscribe", "data": ["channel": channel.name]])
            return
        }
        guard let endpoint = config.authEndpoint else {
            events.offer("e\u{0}\(channel.name)", [
                "k": Self.kindSubscriptionFailed, "c": channel.name, "h": 0, "r": "No auth endpoint configured", "w": -1,
            ])
            return
        }
        channel.authorizing = true
        let owner = generation
        var request = URLRequest(url: endpoint, timeoutInterval: 15)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")
        config.authHeaders.forEach { request.setValue($1, forHTTPHeaderField: $0) }
        if let bearer = config.bearer { request.setValue("Bearer \(bearer)", forHTTPHeaderField: "Authorization") }
        request.httpBody = Data("socket_id=\(Self.formEncode(sid))&channel_name=\(Self.formEncode(channel.name))".utf8)
        URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            guard let self else { return }
            let code = (response as? HTTPURLResponse)?.statusCode ?? 0
            let body = data.map { String(decoding: $0, as: UTF8.self) }
            self.queue.async {
                self.authorized(channel.name, owner: owner, sid: sid, code: code, body: body, error: error?.localizedDescription ?? "Auth HTTP \(code)")
            }
        }.resume()
    }

    private func authorized(_ name: String, owner: Int64, sid: String, code: Int, body: String?, error: String) {
        guard let channel = channels[name] else { return }
        channel.authorizing = false
        guard owner == generation, socketId == sid else { return }
        var auth: [String: Any]?
        if (200..<300).contains(code), let body,
           let object = (try? JSONSerialization.jsonObject(with: Data(body.utf8))) as? [String: Any], object["auth"] != nil {
            auth = object
        }
        guard var auth else {
            failed(channel, status: code, message: (200..<300).contains(code) ? "Invalid auth response" : error)
            return
        }
        auth["channel"] = name
        send(["event": "pusher:subscribe", "data": auth])
    }

    private func failed(_ channel: ChannelState, status: Int, message: String) {
        channel.subscribed = false
        channel.authorizing = false
        channel.authAttempt += 1
        let delay = status == 401 || status == 403
            ? Self.forbiddenRetryMs
            : Self.backoff(channel.authAttempt, floor: Self.authRetryMinMs, ceiling: Self.authRetryMaxMs, multiplier: 2)
        record("Subscription to \(channel.name) failed: \(message)")
        events.offer("e\u{0}\(channel.name)", [
            "k": Self.kindSubscriptionFailed, "c": channel.name, "h": status, "r": message, "w": delay,
        ])
        channel.retry?.cancel()
        let retry = DispatchWorkItem { [weak self, weak channel] in
            guard let self, let channel else { return }
            channel.retry = nil
            if self.channels[channel.name] === channel { self.subscribeChannel(channel) }
        }
        channel.retry = retry
        queue.asyncAfter(deadline: .now() + .milliseconds(Int(delay)), execute: retry)
    }

    // MARK: Liveness and network

    private func startLiveness() {
        liveness?.cancel()
        let period = min(max(activityTimeoutMs / 6, 250), 5_000)
        let timer = DispatchSource.makeTimerSource(queue: queue)
        timer.schedule(deadline: .now() + .milliseconds(Int(period)), repeating: .milliseconds(Int(period)))
        timer.setEventHandler { [weak self] in self?.checkLiveness() }
        timer.resume()
        liveness = timer
    }

    private func checkLiveness() {
        guard state == Self.stateConnected, !closed else { return }
        let now = now()
        if pingSentAt > 0 && now - pingSentAt >= config.pongTimeoutMs {
            lost(generation, "Pong timeout")
        } else if pingSentAt == 0 && now - lastFrameAt >= activityTimeoutMs {
            probe()
        }
    }

    private func probe() {
        guard state == Self.stateConnected, pingSentAt == 0 else { return }
        pingSentAt = now()
        send(["event": "pusher:ping", "data": [String: Any]()])
    }

    private func startNetworkMonitor() {
        let monitor = NWPathMonitor()
        monitor.pathUpdateHandler = { [weak self] path in
            guard let self else { return }
            self.queue.async {
                let previous = self.knownPath
                self.knownPath = path.status
                guard path.status == .satisfied else {
                    if self.state == Self.stateConnected { self.probe() }
                    return
                }
                if self.state == Self.stateUnavailable {
                    // A backoff wait is stale evidence once the network is back.
                    self.openSocket("network")
                } else if self.state == Self.stateConnected, previous != nil {
                    self.probe()
                }
            }
        }
        monitor.start(queue: queue)
        self.monitor = monitor
    }

    // MARK: Helpers

    private func setState(_ next: Int, _ why: String) {
        if state == next && reason == why && next != Self.stateUnavailable { return }
        state = next
        reason = why
        events.offer("state", stateEntry())
    }

    private func stateEntry() -> [String: Any] {
        [
            "k": Self.kindState, "s": state, "r": reason, "i": socketId ?? "", "a": attempt,
            "w": retryAt > 0 ? max(retryAt - now(), 0) : 0,
        ]
    }

    private func snapshot() -> [String: Any] {
        var value = stateEntry()
        value["e"] = lastError
        value["t"] = connectedAt
        value["q"] = events.pending
        value["ch"] = orderedChannels().map { ["n": $0.name, "s": $0.subscribed, "p": $0.authorizing || $0.retry != nil] }
        value["j"] = journal
        return value
    }

    private func record(_ message: String) {
        journal.insert(["at": Int64(Date().timeIntervalSince1970 * 1_000), "m": message], at: 0)
        if journal.count > Self.journalSize { journal.removeLast() }
    }

    private func send(_ payload: [String: Any]) {
        guard let socket, let data = try? JSONSerialization.data(withJSONObject: payload) else { return }
        socket.send(.string(String(decoding: data, as: UTF8.self))) { _ in }
    }

    private func now() -> Int64 { Int64(DispatchTime.now().uptimeNanoseconds / 1_000_000) }

    static func objectData(_ message: [String: Any]) -> [String: Any] {
        switch message["data"] {
        case let object as [String: Any]: return object
        case let text as String:
            return ((try? JSONSerialization.jsonObject(with: Data(text.utf8))) as? [String: Any]) ?? [:]
        default: return [:]
        }
    }

    static func parseData(_ text: String) -> Any {
        let trimmed = text.trimmingCharacters(in: .whitespaces)
        guard trimmed.hasPrefix("{") || trimmed.hasPrefix("[") else { return text }
        return (try? JSONSerialization.jsonObject(with: Data(text.utf8))) ?? text
    }

    static func jsonString(_ value: Any) -> String {
        if let text = value as? String { return text }
        if value is NSNull { return "null" }
        if JSONSerialization.isValidJSONObject(value), let data = try? JSONSerialization.data(withJSONObject: value) {
            return String(decoding: data, as: UTF8.self)
        }
        if let data = try? JSONSerialization.data(withJSONObject: [value]),
           let text = String(data: data, encoding: .utf8), text.count >= 2 {
            return String(text.dropFirst().dropLast())
        }
        return "\(value)"
    }

    static func string(_ value: Any?) -> String {
        switch value {
        case nil, is NSNull: return ""
        case let text as String: return text
        case let number as NSNumber: return number.stringValue
        case let other?: return "\(other)"
        }
    }

    static func formEncode(_ value: String) -> String {
        var allowed = CharacterSet.alphanumerics
        allowed.insert(charactersIn: "-._*")
        return value.addingPercentEncoding(withAllowedCharacters: allowed) ?? value
    }
}
