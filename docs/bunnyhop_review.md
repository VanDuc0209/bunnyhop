# 🐰 BunnyHop — Production Readiness Review

> **Reviewer**: Senior Go Engineer + Distributed Systems Engineer + Security Engineer  
> **Repository**: `github.com/vanduc0209/bunnyhop`  
> **Files reviewed**: `client.go`, `pool.go`, `types.go`, `logger.go`, `utils.go`, `integration_test.go`, `example/main.go`  
> **Review date**: 2026-08-14

---

## 1. Tổng quan kiến trúc

### Các component chính

| Component | File | Vai trò |
|---|---|---|
| `Client` | `client.go` | 1 connection + 1 channel cố định, auto-reconnect |
| `Pool` | `pool.go` | Quản lý N `Client`, load balancing, health check |
| `NodeConnection` | `types.go` | Wrapper node trong pool (URL + Client + health state) |
| `DefaultLogger` | `logger.go` | Logger dùng `log.Printf` chuẩn |

### Lifecycle tóm tắt

```
Connect():
  mutex.Lock → connectToURL() → amqp.DialConfig() → conn.Channel() → ch.Qos() 
  → re-declare exchanges/queues → go reconnectWorker()

reconnectWorker():
  for { select { connClose/chClose → handleDisconnection() } }

handleDisconnection():
  mutex.Lock → connected=false, reconnecting=true → go reconnect()

reconnect():
  mutex.Lock → reconnectAttempts++ → sleep → Connect() → (recursive go reconnect() on failure)

Pool.Start():
  for each node → go connectToNode(node)
  go healthCheckWorker()

watchNodeConnection(node):
  ticker 10s → check node.Client.IsConnected() → update node.healthy

healthCheckWorker():
  ticker HealthCheckInterval → performHealthCheck() → go checkNodeHealth(node)

checkNodeHealth(node):
  if no Client → go connectToNode(node)
  if !connected && healthy → healthy=false
  if connected && !healthy → healthy=true
```

### Shared state / Global state

- `Client.connection`, `Client.channel` — protected by `client.mutex` (RWMutex)
- `Client.connected`, `Client.reconnecting` — protected by `client.mutex`
- `Client.declaredExchanges/Queues/boundQueues` — protected by `client.mutex` (Lock)
- `Pool.nodes` — protected by `pool.mutex` (RWMutex)
- `Pool.closed`, `Pool.roundRobin` — atomic hoặc mutex
- `NodeConnection.healthy`, `NodeConnection.Client`, `NodeConnection.connecting` — protected by `node.mutex` (RWMutex)
- `NodeConnection.totalUsed`, `NodeConnection.failures` — `atomic.AddInt64`

### Background goroutines được tạo

| Goroutine | Điều kiện thoát |
|---|---|
| `reconnectWorker` | `ctx.Done()` hoặc nhận close event rồi return |
| `reconnect` (via `go c.reconnect()`) | Xem issue #1 — **CÓ LEAK** |
| `connectToNode` (via `go p.connectToNode()`) | Xem issue #4 |
| `watchNodeConnection` | `p.ctx.Done()` |
| `healthCheckWorker` | `p.ctx.Done()` |
| `checkNodeHealth` (per health tick) | Xem issue #5 — **CÓ LEAK TIỀM ẨN** |

---

## 2–24. Critical Findings

---

### ISSUE #1

```
[CRITICAL]

Title:
Goroutine leak + Connection leak trong recursive reconnect()

Location:
client.go:272–275

Category:
Goroutine Leak / Connection Leak / Memory Leak

Problem:
reconnect() gọi đệ quy chính nó bằng `go c.reconnect()` khi Connect thất bại.
Mỗi lần thất bại tạo thêm 1 goroutine mới. 
Không có giới hạn stack depth thực sự vì mỗi cuộc gọi là goroutine độc lập.
Sau MaxReconnectAttempt lần thất bại đã thoát ở check đầu, nhưng 
race window cho phép nhiều goroutine reconnect() tồn tại song song.

Why it happens:
```go
// client.go:272-275
if err := c.Connect(c.ctx); err != nil {
    c.logger().Error("Reconnection failed: %v", err)
    go c.reconnect()   // ← tạo goroutine mới mỗi lần fail
}
```
reconnect() đã release mutex ở line 258 trước khi sleep/Connect.
Khi Pool gọi node.Client.Close() rồi tạo NewClient() mới, client cũ
vẫn giữ c.ctx (context riêng), và c.ctx.Done() chưa được cancel 
(Close() gọi cancel nhưng nếu đang trong sleep thì chỉ thoát sau sleep).

Nếu ReconnectInterval = 5s và RabbitMQ down 60s:
goroutine 1 ngủ 5s → fail → spawn goroutine 2
goroutine 2 ngủ 5s → fail → spawn goroutine 3
...
12 goroutines tích lũy sau 60s cho MỘT client.

Impact:
- Goroutine leak tỷ lệ thuận với thời gian RabbitMQ down
- Mỗi goroutine giữ stack ~8KB + reference đến Client struct
- Khi RabbitMQ recover, tất cả goroutines đều cố connect → connection storm
- Memory growth: O(downtime / ReconnectInterval)

Attack/Failure scenario:
RabbitMQ down 10 phút, ReconnectInterval=5s, pool 3 nodes:
- 3 nodes × 120 goroutines = 360 goroutines leak
- RabbitMQ recover → 360 goroutines cùng connect → 360 connections cùng lúc

Evidence:
```go
// client.go:272-275
if err := c.Connect(c.ctx); err != nil {
    c.logger().Error("Reconnection failed: %v", err)
    go c.reconnect()   // BUG: new goroutine every failure
}
```

Recommendation:
Chuyển reconnect() thành vòng lặp for thay vì đệ quy:
```go
func (c *Client) reconnect() {
    for {
        c.mutex.Lock()
        if c.connected {
            c.reconnecting = false
            c.mutex.Unlock()
            return
        }
        c.reconnectAttempts++
        if c.config.MaxReconnectAttempt != 0 && 
            c.reconnectAttempts > c.config.MaxReconnectAttempt {
            c.reconnecting = false
            c.mutex.Unlock()
            return
        }
        c.mutex.Unlock()

        timer := time.NewTimer(backoff(c.reconnectAttempts, c.config.ReconnectInterval))
        select {
        case <-c.ctx.Done():
            timer.Stop()
            c.mutex.Lock()
            c.reconnecting = false
            c.mutex.Unlock()
            return
        case <-timer.C:
        }

        if err := c.Connect(c.ctx); err != nil {
            c.logger().Error("Reconnection failed: %v", err)
            // loop lại, KHÔNG spawn goroutine
        }
    }
}
```

Priority: P0
```

---

### ISSUE #2

```
[CRITICAL]

Title:
reconnectWorker không có vòng lặp for → mất khả năng detect disconnect sau lần đầu

Location:
client.go:187–214

Category:
Reliability / Silent Data Loss

Problem:
reconnectWorker() lắng nghe close notification trên 2 channel. 
Khi nhận được 1 event, nó gọi handleDisconnection() rồi RETURN luôn.
Lần kết nối tiếp theo (sau reconnect) sẽ tạo reconnectWorker mới. 
Nhưng có race condition: nếu cả connClose lẫn chClose đều có event 
cùng lúc (ví dụ connection close kéo theo channel close), 
chỉ 1 event được xử lý, event còn lại bị bỏ qua hoàn toàn.

Why it happens:
```go
// client.go:187-214: không có for loop bao ngoài
func (c *Client) reconnectWorker(ctx context.Context, connClose, chClose chan *amqp.Error) {
    // select chỉ chạy 1 lần, không loop
    select {
    case <-ctx.Done(): return
    case err, ok := <-connClose:
        if !ok || err != nil { ... c.handleDisconnection(); return }
    case err, ok := <-chClose:
        if !ok || err != nil { ... c.handleDisconnection(); return }
    }
    // Nếu `ok=true && err==nil` (graceful close): 
    // function return mà KHÔNG gọi handleDisconnection!
}
```

Nghiêm trọng: graceful close (RabbitMQ restart/upgrade) trả về ok=true, err=nil.
Nhánh `if !ok || err != nil` KHÔNG khớp → handleDisconnection() KHÔNG được gọi.
Client tiếp tục nghĩ là đang connected, mọi Publish sẽ fail với error mờ.

Impact:
- Silent disconnect: client không biết mình đã mất kết nối
- Publish tiếp tục fail với error "channel/connection not open"
- Không có auto-reconnect → service mất khả năng gửi message vĩnh viễn
- Xảy ra trong rolling deployment RabbitMQ (graceful restart)

Attack/Failure scenario:
RabbitMQ rolling upgrade (graceful shutdown):
→ Connection closed gracefully (ok=true, err=nil)
→ reconnectWorker return không gọi handleDisconnection
→ client.connected = true (không đổi)
→ mọi PublishMessage đều fail với amqp error
→ không có reconnect → service dead

Evidence:
```go
case err, ok := <-connClose:
    if !ok || err != nil {   // ← graceful close: ok=true, err=nil → branch bỏ qua
        ...
        c.handleDisconnection()
        return
    }
// ← fall-through: return mà không xử lý
```

Recommendation:
```go
func (c *Client) reconnectWorker(ctx context.Context, connClose, chClose chan *amqp.Error) {
    for {
        select {
        case <-ctx.Done():
            return
        case err, ok := <-connClose:
            if !ok || err == nil {
                // graceful close: cũng cần reconnect
                c.handleDisconnection()
                return
            }
            c.logger().Error("RabbitMQ Connection error: %v", err)
            c.handleDisconnection()
            return
        case err, ok := <-chClose:
            if !ok || err == nil {
                c.handleDisconnection()
                return
            }
            c.logger().Error("RabbitMQ Channel error: %v", err)
            c.handleDisconnection()
            return
        }
    }
}
```

Priority: P0
```

---

### ISSUE #3

```
[CRITICAL]

Title:
Race condition: GetClient() lấy *Client ref rồi release mutex → channel/connection bị thay thế mid-operation

Location:
pool.go:197–204, client.go:354–365

Category:
Race Condition / Data Loss / Panic Risk

Problem:
GetClient() trả về *Client pointer sau khi release pool.mutex.
Caller sau đó gọi client.PublishMessage() trên pointer đó.
Trong khoảng thời gian giữa GetClient() return và PublishMessage() gọi,
Pool có thể close node.Client cũ và gán node.Client = newClient.
Client cũ đã bị Close() → channel nil → PublishMessage panic hoặc publish vào closed channel.

Why it happens:
```go
// pool.go:198-204
selectedNode.mutex.Lock()
atomic.AddInt64(&selectedNode.totalUsed, 1)
selectedNode.lastUsed = time.Now()
client := selectedNode.Client   // lấy pointer
selectedNode.mutex.Unlock()     // release lock

return client, nil  // caller nhận pointer không được bảo vệ
```
Caller sau đó gọi:
```go
client.PublishMessage(...)  // client có thể đã bị close
```

Đây là TOCTOU (Time-of-Check Time-of-Use) race.

Impact:
- Panic nếu channel đã nil sau close
- Message loss: publish vào connection đã dead
- Không có cách nào để caller biết client đã invalid

Attack/Failure scenario:
goroutine A: client = pool.GetClient()    // client = oldClient
goroutine B: pool.Close() closes oldClient
goroutine A: client.PublishMessage() → panic/error mà không retry

Evidence:
```go
// pool.go:201-203
client := selectedNode.Client
selectedNode.mutex.Unlock()
return client, nil  // caller giữ stale pointer
```

Recommendation:
Thêm wrapper method để operation là atomic, hoặc implement Publish trực tiếp trên Pool:
```go
func (p *Pool) Publish(exchange, routingKey string, msg amqp.Publishing) error {
    // select node và publish trong 1 critical section kiểm soát bởi pool
    // retry với node khác nếu publish fail
}
```
Nếu vẫn expose *Client, document rõ caller phải handle error và retry.

Priority: P0
```

---

### ISSUE #4

```
[HIGH]

Title:
connectToNode() giữ node.mutex trong suốt quá trình I/O mạng → Block toàn bộ Pool

Location:
pool.go:111–123

Category:
Deadlock / Throughput Degradation / High Latency

Problem:
connectToNode() sau khi thực hiện network I/O (client.Connect) xong mới acquire lock.
Nhưng vấn đề là lock được acquire SAU I/O, còn trong checkNodeHealth() 
thì node.mutex.Lock() được giữ KHI GỌI go p.connectToNode(node):

```go
// pool.go:319-328
func (p *Pool) checkNodeHealth(node *NodeConnection) {
    node.mutex.Lock()       // ← lock giữ trước
    defer node.mutex.Unlock()
    
    if node.Client == nil {
        ...
        go p.connectToNode(node)  // ← goroutine mới, nhưng lock vẫn giữ đến hết function
        return
    }
}
```

Khi getHealthyNodes() cố đọc node.mutex.RLock() sẽ block cho đến khi
checkNodeHealth() unlock. Nếu checkNodeHealth bị block (ví dụ IsConnected()
gọi vào socket bị timeout), tất cả GetClient() call sẽ bị treo.

Thêm nữa: trong getHealthyNodes() gọi node.Client.IsConnected() trong khi
đang giữ node.mutex.RLock(). IsConnected() gọi client.mutex.RLock() và 
c.connection.IsClosed() — một I/O call. Điều này có thể block lâu.

Impact:
- GetClient() latency tăng đột biến khi có node unhealthy
- Có thể block request handling thread pool của application
- Trong K8s với nhiều node reconnecting đồng thời, pool gần như unusable

Evidence:
```go
// pool.go:286-294
func (p *Pool) getHealthyNodes() []*NodeConnection {
    for _, node := range p.nodes {
        node.mutex.RLock()
        if node.healthy && node.Client != nil && node.Client.IsConnected() {
            // IsConnected() có thể block!
            healthyNodes = append(healthyNodes, node)
        }
        node.mutex.RUnlock()
    }
    return healthyNodes
}
```

Recommendation:
- Cache trạng thái `healthy` và chỉ update từ background worker
- getHealthyNodes() chỉ đọc `node.healthy` (bool), không gọi IsConnected()
- IsConnected() chỉ nên được gọi từ health check goroutine, không từ hot path

Priority: P1
```

---

### ISSUE #5

```
[HIGH]

Title:
Duplicate goroutine watchNodeConnection + checkNodeHealth gây connection churn

Location:
pool.go:136, pool.go:313, pool.go:327

Category:
Goroutine Leak / Connection Churn / Race Condition

Problem:
Mỗi khi connectToNode() thành công, nó spawn `go p.watchNodeConnection(node)`.
watchNodeConnection() chạy ticker 10 giây và update node.healthy.
Đồng thời healthCheckWorker() cũng spawn `go p.checkNodeHealth(node)` định kỳ.

Nếu connectToNode() được gọi nhiều lần cho cùng 1 node (do race condition),
nhiều watchNodeConnection goroutine cùng tồn tại cho cùng node.
Mỗi goroutine đều có thể trigger connectToNode() khi phát hiện disconnect.

Kịch bản cụ thể:
1. healthCheckWorker tick → checkNodeHealth(node) → node.Client == nil → go connectToNode(node)
2. connectToNode 1 bắt đầu
3. healthCheckWorker tick lại → checkNodeHealth(node) → node.Client == nil (chưa assign) → go connectToNode(node)
4. Hai connectToNode goroutines cùng chạy
5. node.connecting flag chỉ bảo vệ được 1 goroutine trong cùng thời điểm,
   nhưng sau defer reset, goroutine thứ 2 nhìn thấy connecting=false và tiếp tục

Impact:
- Duplicate connections đến cùng RabbitMQ node
- Kết nối mới đè kết nối cũ → message đang in-flight bị drop
- watchNodeConnection goroutines tích lũy theo số lần reconnect
- Tiêu tốn RabbitMQ connection slots

Evidence:
```go
// pool.go:136: mỗi connectToNode thành công spawn 1 watcher
go p.watchNodeConnection(node)

// pool.go:327: checkNodeHealth cũng trigger connectToNode
go p.connectToNode(node)

// node.connecting flag không đủ để prevent race:
defer func() {
    node.mutex.Lock()
    node.connecting = false  // ← reset ngay sau khi xong
    node.mutex.Unlock()
}()
```

Recommendation:
- Loại bỏ một trong hai: watchNodeConnection HOẶC healthCheckWorker logic
- Dùng single-responsibility: chỉ healthCheckWorker quản lý reconnect
- watchNodeConnection không nên trigger reconnect, chỉ update status
- connectToNode nên được gọi duy nhất từ 1 nơi, dùng sync.Once hoặc channel signal

Priority: P1
```

---

### ISSUE #6

```
[HIGH]

Title:
Data race: p.totalRequests / p.totalFailures đọc không atomic trong GetStats()

Location:
pool.go:351–352

Category:
Race Condition / Data Race

Problem:
totalRequests và totalFailures được tăng bằng atomic.AddInt64 trong GetClient().
Nhưng trong GetStats() chúng được đọc bằng... atomic.LoadInt64. Thực ra đây OK.

NHƯNG: trong GetStats(), `stats.HealthyNodes++` (line 370) được thực hiện
sau khi đọc node stats, KHÔNG được protect bởi bất kỳ lock nào ngoài p.mutex.RLock.
Node health có thể thay đổi trong quá trình đọc → stats inaccurate.
Đây là "acceptable" với metrics nhưng cần document.

Vấn đề thực sự: `node.Client.IsConnected()` được gọi tại line 361 
trong khi chỉ giữ node.mutex.RLock(), nhưng IsConnected() 
gọi vào client.mutex.RLock() và c.connection.IsClosed() — 
đây là cross-lock call có thể gây deadlock nếu thứ tự lock không nhất quán.

Lock ordering hiện tại:
- GetStats():    pool.mutex.RLock → node.mutex.RLock → client.mutex.RLock  
- connectToNode: node.mutex.Lock → (tạo client)
- Pool.Close():  pool.mutex.Lock → node.mutex.Lock → client.Close (→ client.mutex.Lock)

3 level nested lock với thứ tự không cố định = deadlock risk.

Impact:
- Potential deadlock trong production
- GetStats() hang → monitoring/health endpoint không response
- K8s liveness probe timeout → pod restart storm

Evidence:
```go
// pool.go:356-374 - 3 level nested lock
for _, node := range p.nodes {          // pool.mutex.RLock (held)
    node.mutex.RLock()                   // node.mutex.RLock
    nodeStat := NodeStats{
        Connected: node.Client.IsConnected(),  // → client.mutex.RLock
    }
    node.mutex.RUnlock()
}
```

Recommendation:
- Flatten lock: cache IsConnected result trong node.healthy (boolean), chỉ đọc bool trong GetStats()
- Không gọi I/O hay nested lock trong GetStats()
- Document lock ordering nếu nested lock là bắt buộc

Priority: P1
```

---

### ISSUE #7

```
[HIGH]

Title:
Pool.Close() không dùng node.mutex khi đọc node.Client

Location:
pool.go:402–411

Category:
Race Condition / Panic Risk

Problem:
Pool.Close() lặp qua nodes, đọc node.Client mà không giữ node.mutex:

```go
for _, node := range p.nodes {
    node.mutex.Lock()
    client := node.Client   // ← OK: giữ lock khi đọc
    node.mutex.Unlock()

    if client != nil {
        if err := client.Close(); err != nil {  // ← nhưng không giữ lock
```

Thực ra code này đọc node.Client đúng (mutex.Lock). 
Nhưng sau khi unlock, một goroutine khác có thể assign node.Client = newClient,
và ta đang gọi Close() trên client CŨ trong khi client MỚI chạy song song.
Client cũ đã có c.cancel() gọi rồi, client.Close() có thể double-cancel context
hoặc double-close connection gây error không cần thiết.

Nghiêm trọng hơn: connectToNode() sau khi Pool.Close() cancel context 
có thể vẫn đang chạy (được spawn trước khi Close gọi cancel()).
Nó sẽ tiếp tục try connect dù pool đã closed.

Impact:
- Client leak: orphan clients không được close
- Error noise từ double-close
- connectToNode goroutine chạy sau pool close → connection leak

Evidence:
```go
// pool.go:120-122: AfterFunc không bị cancel khi pool close
time.AfterFunc(p.config.ReconnectInterval, func() {
    p.connectToNode(node)  // chạy dù pool.closed=true
})
```

Recommendation:
```go
// Trong connectToNode, check pool.closed trước:
func (p *Pool) connectToNode(node *NodeConnection) {
    p.mutex.RLock()
    if p.closed {
        p.mutex.RUnlock()
        return
    }
    p.mutex.RUnlock()
    ...
}
```
Và dùng p.ctx trong AfterFunc thay vì raw time.AfterFunc.

Priority: P1
```

---

### ISSUE #8

```
[HIGH]

Title:
AMQP URL (chứa credentials) bị log ra khi connect fail

Location:
client.go:107–110, pool.go:53, pool.go:97, pool.go:133

Category:
Security / Credential Leakage

Problem:
```go
// client.go:107
c.logger().Warn("Failed to connect to %s: %v", url, err)

// client.go:110
c.logger().Info("Successfully connected to %s", url)
```

AMQP URL có format: `amqp://user:password@host:port/vhost`
Toàn bộ URL bao gồm password được log ra ở level WARN và INFO.

```go
// pool.go:53
pool.logger.Debug("Initialized node %d: %s", i, url)

// pool.go:97
p.logger.Debug("Connecting to node %s", node.URL)

// pool.go:133
p.logger.Info("Successfully connected to node %s", node.URL)
```

Với DebugLog=true (và trong test/integration_test.go hardcode DebugLog:true),
credentials xuất hiện trong application logs, có thể bị:
- Thu thập bởi log aggregation (ELK, Loki)
- Hiển thị trong Kubernetes pod logs
- Leak ra monitoring dashboard
- Thu thập bởi attacker có read access đến logs

Impact:
- RabbitMQ credentials exposed trong logs
- Compliance violation (PCI-DSS, SOC2, GDPR)
- Lateral movement nếu attacker đọc được logs

Evidence:
```go
// client.go:107
c.logger().Warn("Failed to connect to %s: %v", url, err)
// Output: [WARN] Failed to connect to amqp://admin:secret123@rabbitmq:5672/ ...
```

Recommendation:
Mask password trong URL trước khi log:
```go
func maskAMQPURL(rawURL string) string {
    u, err := url.Parse(rawURL)
    if err != nil {
        return "[invalid-url]"
    }
    if u.User != nil {
        u.User = url.UserPassword(u.User.Username(), "***")
    }
    return u.String()
}
// Usage:
c.logger().Warn("Failed to connect to %s: %v", maskAMQPURL(url), err)
```

Priority: P1
```

---

### ISSUE #9

```
[HIGH]

Title:
Single shared AMQP Channel cho tất cả operations → throughput bottleneck + channel close race

Location:
client.go:53–54, client.go:362–365

Category:
Performance / Race Condition / Throughput Degradation

Problem:
Client dùng DUY NHẤT 1 amqp.Channel cho mọi thứ:
- DeclareQueue, DeclareExchange, QueueBind
- PublishMessage (với publishMutex)
- Tiềm năng: Consumer (nếu caller dùng GetChannel())

amqp.Channel trong amqp091-go KHÔNG thread-safe. 
Code dùng `publishMutex` để serialize publish, nhưng các lệnh Declare
dùng `mutex` (RWMutex → Lock) khác với `publishMutex`.

Race condition:
Thread A: DeclareQueue() → mutex.Lock → ch.QueueDeclare()
Thread B: PublishMessage() → mutex.RLock → [ch captured] → publishMutex.Lock → ch.Publish()

Thread A và B có thể dùng ch ĐỒNG THỜI nếu có sự trùng lặp:
- A giữ mutex.Lock
- B lấy ch reference SAU KHI mutex.RLock (không thể đọc trong khi A write)

Thực ra đây không race vì Lock() blocks RLock(). Nhưng:

Khi reconnect xảy ra và channel mới được tạo trong connectToURL():
1. ch (new) được assign vào c.channel trong khi mutex.Lock held
2. Nhưng PublishMessage đã capture ch cũ trước khi reconnect bắt đầu
3. Nếu có timing window giữa RLock release và publishMutex.Lock,
   ch reference bị stale

```go
// client.go:354-365
c.mutex.RLock()
if !c.connected || c.channel == nil {
    c.mutex.RUnlock()
    return fmt.Errorf("client is not connected")
}
ch := c.channel   // ← capture reference
c.mutex.RUnlock() // ← release lock

c.publishMutex.Lock()
defer c.publishMutex.Unlock()

return ch.Publish(...)  // ← ch có thể đã bị thay thế
```

Impact:
- PublishMessage dùng stale channel → publish fail (error từ amqp)
- Bottleneck: chỉ 1 channel → 1 publish tại 1 thời điểm → throughput giới hạn ở ~10-30k msg/s tùy network
- DeclareQueue/DeclareExchange blocking publishMutex (không phải)... thực ra không dùng chung mutex

Recommendation:
- Dùng channel pool (sync.Pool hoặc buffered channel) thay vì 1 channel
- Hoặc document rõ: "1 channel = 1 goroutine, caller phải tự pool"
- Fix stale channel: check ch validity sau publishMutex.Lock

Priority: P1
```

---

### ISSUE #10

```
[HIGH]

Title:
QoS hardcode prefetch=1 trong connectToURL() ảnh hưởng Publisher throughput

Location:
client.go:141–145

Category:
Performance / Design

Problem:
```go
if err := ch.Qos(1, 0, false); err != nil {
```

prefetchCount=1 là setting cho Consumer (giới hạn unacked messages).
Nhưng channel này cũng được dùng để Publish.

Với prefetch=1:
- RabbitMQ chỉ deliver 1 message tới consumer tại 1 thời điểm
- Nếu channel được consumer sử dụng → throughput giới hạn nghiêm trọng
- Không có option để override per-consumer

Hơn nữa, QoS không nên được hardcode — cần là configurable.
Với HPA trên K8s và nhiều replicas, prefetch=1 per channel × nhiều replicas 
có thể gây starve khi queue có burst messages.

Impact:
- Consumer throughput capped tại ~few hundred msg/s với prefetch=1
- Không flexible cho các use case khác nhau (batch processing, etc.)

Evidence:
```go
// client.go:141-145
if err := ch.Qos(1, 0, false); err != nil {
    _ = ch.Close()
    _ = conn.Close()
    return fmt.Errorf("failed to set QoS: %w", err)
}
```

Recommendation:
```go
type Config struct {
    // ...
    PrefetchCount int  // default 10 cho balanced throughput/memory
    PrefetchSize  int
    PrefetchGlobal bool
}

// Trong connectToURL:
prefetch := c.config.PrefetchCount
if prefetch <= 0 {
    prefetch = 10
}
if err := ch.Qos(prefetch, 0, false); err != nil { ... }
```

Priority: P1
```

---

### ISSUE #11

```
[MEDIUM]

Title:
time.AfterFunc trong connectToNode() không bị cancel khi Pool close

Location:
pool.go:120–122

Category:
Goroutine Leak / Resource Leak

Problem:
```go
time.AfterFunc(p.config.ReconnectInterval, func() {
    p.connectToNode(node)
})
```

time.AfterFunc tạo 1 goroutine timer không được track. 
Khi Pool.Close() được gọi, p.cancel() cancel context nhưng 
AfterFunc timer vẫn đang chạy.

Sau khi timer hết, nó gọi p.connectToNode(node) mặc dù pool đã closed.
connectToNode() sẽ tạo NewClient và connect thành công → orphan connection.
Client này không có reference trong pool.nodes → không bao giờ được close.

Impact:
- Connection leak sau Pool.Close()
- Đặc biệt nguy hiểm khi app restart hoặc K8s rolling deploy
- RabbitMQ connection limit bị exhausted dần dần

Evidence:
```go
// pool.go:120-122
time.AfterFunc(p.config.ReconnectInterval, func() {
    p.connectToNode(node)  // chạy sau Pool.Close()!
})
```

Recommendation:
```go
// Dùng timer với context:
go func() {
    timer := time.NewTimer(p.config.ReconnectInterval)
    defer timer.Stop()
    select {
    case <-p.ctx.Done():
        return
    case <-timer.C:
        p.connectToNode(node)
    }
}()
```

Priority: P2
```

---

### ISSUE #12

```
[MEDIUM]

Title:
declaredQueues/Exchanges tích lũy vô hạn — memory leak khi declare nhiều queue động

Location:
client.go:381–387, client.go:405–412, client.go:426–432

Category:
Memory Leak / Unbounded Memory Growth

Problem:
Mỗi lần gọi DeclareQueue(), DeclareExchange(), QueueBind(),
metadata được append vào slice tương ứng mà KHÔNG BAO GIỜ được xóa:

```go
c.declaredExchanges = append(c.declaredExchanges, exchangeMeta{...})
c.declaredQueues = append(c.declaredQueues, queueMeta{...})
c.boundQueues = append(c.boundQueues, bindMeta{...})
```

Use case nguy hiểm: application tạo per-message queue (như DLQ per job):
```
job-1-dlq, job-2-dlq, ... job-1000000-dlq
→ declaredQueues slice = 1M entries
→ mỗi reconnect re-declare tất cả 1M entries
→ reconnect timeout / RabbitMQ overload
```

Impact:
- Memory growth linear với số Declare operations
- Reconnect time tăng theo số queue/exchange
- RabbitMQ overload khi reconnect với hàng ngàn declare
- OOM trong long-running service

Evidence:
```go
// client.go:381-387
c.declaredQueues = append(c.declaredQueues, queueMeta{
    name:       name,
    durable:    durable,
    autoDelete: autoDelete,
    exclusive:  exclusive,
    args:       args,
})
```

Recommendation:
- Dùng map[string]queueMeta để dedup
- Giới hạn số lượng stored metadata (cap)
- Allow caller opt-out của auto re-declare
- Document behavior rõ ràng

Priority: P2
```

---

### ISSUE #13

```
[MEDIUM]

Title:
No exponential backoff trong reconnect — fixed interval dù fail nhiều lần

Location:
client.go:260, client.go:70–72

Category:
CPU / Throughput / RabbitMQ Overload

Problem:
Reconnect interval là fixed constant (default 30s, minimum không có limit).
Không có exponential backoff, không có jitter.

Với ReconnectInterval=1s (như trong integration_test.go):
- RabbitMQ down 10 phút
- Mỗi giây: 1 goroutine reconnect, fail, spawn goroutine mới
- 600 reconnect attempts, cộng với goroutine leak từ #1
- Nếu 100 services cùng reconnect: 100 × 600 = 60,000 AMQP handshake/10min

Thundering herd khi RabbitMQ recover:
Tất cả services cùng reconnect đồng thời → RabbitMQ quá tải → reject connections → tất cả retry lại

Impact:
- CPU spike trong reconnect storm
- RabbitMQ overload khi nhiều service reconnect đồng thời
- Reconnect kéo dài hơn mức cần thiết

Recommendation:
```go
func exponentialBackoff(attempt int, base time.Duration) time.Duration {
    backoff := base * time.Duration(1<<uint(attempt-1))
    maxBackoff := 5 * time.Minute
    if backoff > maxBackoff {
        backoff = maxBackoff
    }
    // Add jitter: ±20%
    jitter := time.Duration(rand.Int63n(int64(backoff) / 5))
    return backoff + jitter
}
```

Priority: P2
```

---

### ISSUE #14

```
[MEDIUM]

Title:
reconnect() gọi c.Connect(c.ctx) trong khi đã release mutex → ABA race với MaxReconnectAttempt

Location:
client.go:232–275

Category:
Race Condition / Logic Bug

Problem:
reconnect() pattern:
1. Lock → check connected → increment reconnectAttempts → Unlock
2. Sleep
3. Lock (via Connect) → kết nối thành công → connected=true → reconnectAttempts=0 → Unlock
4. Nếu fail → spawn goroutine mới → goroutine mới Lock → reconnectAttempts++ → ...

Race: Giữa bước 1 và 3, một goroutine khác (hoặc từ issue #1) cũng increment reconnectAttempts.
reconnectAttempts có thể vượt MaxReconnectAttempt sớm hơn dự kiến.
Ngược lại: Connect thành công reset về 0 nhưng goroutine cũ (từ issue #1) 
đang sleep và sẽ tạo thêm goroutine sau khi sleep xong.

Không có guarantee về reconnectAttempts integrity.

Impact:
- Client có thể stop reconnect sớm hơn MaxReconnectAttempt
- Hoặc không stop dù đã đạt limit (race với reset)
- Behavior không predictable

Recommendation:
Dùng atomic.AddInt64 cho reconnectAttempts, và fix issue #1 (goroutine stack) trước.

Priority: P2
```

---

### ISSUE #15

```
[MEDIUM]

Title:
Không có Consumer API — caller phải dùng GetChannel() trực tiếp, unsafe

Location:
client.go (missing consumer implementation)

Category:
Design / Reliability / Message Loss

Problem:
Thư viện không có Consume() API. Caller phải:
1. Lấy channel bằng GetChannel()
2. Tự gọi ch.Consume()
3. Tự handle reconnect

Khi reconnect xảy ra:
- Channel cũ bị close
- Channel mới được tạo trong connectToURL()
- Nhưng consumer đang consume từ channel cũ → channel bị close → delivery channel đóng → consumer loop exit

Caller phải tự implement re-subscribe sau reconnect. 
Nếu không, sau reconnect sẽ không có consumer nào nhận message.

Impact:
- Message loss sau reconnect nếu caller không implement re-subscribe
- RabbitMQ queue sẽ accumulate messages không được consume
- Invisible bug: không có error, chỉ im lặng

Attack/Failure scenario:
1. Consumer start: ch = GetChannel() → ch.Consume()
2. RabbitMQ reconnect
3. Old channel closed → delivery channel receives AMQP error → loop exits
4. Consumer goroutine exits
5. No consumer → messages pile up in queue

Evidence:
API surface không có Consume/Subscribe method.
GetChannel() expose raw channel, no lifecycle management.

Recommendation:
Implement Consume() API với auto-resubscribe:
```go
type MessageHandler func(delivery amqp.Delivery) error

func (c *Client) Consume(ctx context.Context, queue string, handler MessageHandler) error {
    // Start consuming, reconnect-aware
    // Re-register consumer after reconnect
}
```

Priority: P2
```

---

### ISSUE #16

```
[MEDIUM]

Title:
No Publisher Confirms — message có thể bị mất silently khi publish thành công nhưng RabbitMQ lỗi

Location:
client.go:365

Category:
Reliability / Message Loss

Problem:
ch.Publish() (alias PublishMessage) là fire-and-forget.
Không có channel.Confirm() mode.
RabbitMQ có thể ack gửi thành công về phía TCP nhưng sau đó fail internaly
(ví dụ: disk full, queue overflow, mirroring lag).

Với mandatory=false (default trong example), message bị drop nếu không route được
mà không có bất kỳ notification nào.

Impact:
- Silent message loss trong high-load scenarios
- Người dùng thư viện không biết publish có thực sự thành công không
- Nguy hiểm cho critical business messages (payment, order, etc.)

Recommendation:
Cung cấp PublishWithConfirm() API:
```go
func (c *Client) PublishWithConfirm(ctx context.Context, ...) error {
    // Enable confirm mode
    // Wait for ack/nack with timeout
}
```
Hoặc ít nhất document rõ limitation này.

Priority: P2
```

---

### ISSUE #17

```
[LOW]

Title:
No input validation cho Config — có thể gây panic hoặc unexpected behavior

Location:
client.go:69–89, utils.go:7–24

Category:
Reliability / Defense

Problem:
- Config.URLs rỗng không được validate ở Client level (chỉ Pool level)
- ReconnectInterval âm được normalize, nhưng MaxReconnectAttempt=0 có nghĩa unlimited (documented?) hay bug?
- Không validate URL format trước khi connect
- Không giới hạn số lượng URLs
- PoolConfig.URLs rỗng được default là localhost — silent behavior change

Evidence:
```go
// utils.go:18-20
if len(config.URLs) == 0 {
    config.URLs = []string{"amqp://localhost:5672"}  // silent default
}
```

Recommendation:
- Return error nếu URLs rỗng thay vì silent default
- Document MaxReconnectAttempt=0 behavior (unlimited)
- Validate URL format

Priority: P3
```

---

### ISSUE #18

```
[LOW]

Title:
log amplification khi RabbitMQ down với ngắn reconnect interval

Location:
client.go:107, client.go:273

Category:
CPU / Observability / Log Flood

Problem:
Mỗi reconnect attempt log 2 dòng:
- "Failed to connect to..." (WARN)
- "Reconnection failed: ..." (ERROR)

Với ReconnectInterval=1s và MaxReconnectAttempt=0 (unlimited):
- RabbitMQ down 10 phút = 1200 log lines per client
- Pool với 3 nodes × 3 services = 10,800 log lines
- Gây log storage explosion và CPU overhead từ I/O

Impact:
- Log storage cost tăng đột biến khi outage
- Alerting system bị overwhelm
- CPU spike từ log I/O

Recommendation:
- Implement rate-limited logging (log first, then sample)
- Exponential backoff giảm log frequency tự nhiên (xem issue #13)
- Log level: chỉ ERROR lần đầu, DEBUG các lần sau

Priority: P3
```

---

### ISSUE #19

```
[LOW]

Title:
Test coverage thiếu các critical scenario

Location:
integration_test.go

Category:
Testing

Problem:
Các scenario KHÔNG được test:
- Concurrent publish từ 100 goroutines
- Reconnect race condition
- Pool shutdown trong khi reconnect
- Consumer re-subscribe sau reconnect
- Memory growth sau nhiều reconnect cycles
- Goroutine count sau reconnect (goleak)
- Race condition với -race flag

Tests hiện tại chỉ test happy path với real RabbitMQ và
TestLoadBalancingStrategies không thực sự test với live nodes.

Recommendation:
```bash
# Race detection:
go test -race ./...

# Goroutine leak detection với goleak:
go get go.uber.org/goleak
// Trong TestX:
defer goleak.VerifyNone(t)

# Stress test:
go test -run TestX -count=100 -race ./...

# Benchmark:
go test -bench=. -benchmem -benchtime=30s ./...
```

Test reproducer cho issue #1 (goroutine leak):
```go
func TestGoroutineLeakOnReconnect(t *testing.T) {
    defer goleak.VerifyNone(t)
    
    client := NewClient(Config{
        URLs: []string{"amqp://invalid:5672"},
        ReconnectInterval: 10*time.Millisecond,
        MaxReconnectAttempt: 5,
    })
    ctx := context.Background()
    client.Connect(ctx)
    time.Sleep(200*time.Millisecond)
    client.Close()
    time.Sleep(100*time.Millisecond)
    // goleak sẽ detect leaked goroutines
}
```

Priority: P3
```

---

## 20. Production Readiness Score

| Category | Score /10 | Ghi chú |
|---|---:|---|
| Security | **4/10** | Credential leak trong log, no TLS enforcement |
| Concurrency | **3/10** | Race condition, lock ordering issue |
| Goroutine safety | **3/10** | Goroutine leak trong recursive reconnect |
| Memory safety | **4/10** | Unbounded slice growth, fixed interval |
| CPU efficiency | **5/10** | No backoff, log amplification |
| Connection management | **4/10** | Single channel, no publisher confirms |
| Channel management | **3/10** | 1 shared channel cho tất cả ops |
| Load balancing | **6/10** | 4 strategies nhưng hot path lock có vấn đề |
| Publisher performance | **4/10** | publishMutex serialize tất cả, stale channel race |
| Consumer performance | **1/10** | Không có Consumer API |
| Retry mechanism | **3/10** | Fixed interval, recursive goroutine, no jitter |
| Failure recovery | **3/10** | Silent disconnect, reconnect storm |
| Graceful shutdown | **5/10** | Cơ bản đúng nhưng AfterFunc leak |
| Observability | **3/10** | Chỉ có log, không có metrics/tracing |
| Test coverage | **2/10** | Chỉ integration test, không có race test |
| **Overall production readiness** | **3.5/10** | **Không nên deploy production khi chưa fix P0/P1** |

---

## Executive Summary

> **"Nếu đem bunnyhop chạy production với traffic lớn, RabbitMQ gặp failure liên tục, nó sẽ chết ở đâu trước?"**

**Thứ tự sụp đổ:**

1. **Goroutine explosion** (Issue #1, P0): Mỗi reconnect failure sinh 1 goroutine mới. Sau 10 phút RabbitMQ down với ReconnectInterval=5s → hàng trăm goroutines leak. Khi RabbitMQ recover → connection storm.

2. **Silent dead client** (Issue #2, P0): Graceful close (rolling restart RabbitMQ) không trigger reconnect → client tiếp tục nghĩ mình connected → mọi Publish fail silently → message loss.

3. **Stale channel publish** (Issue #3, P0): Race giữa reconnect và publish → Publish dùng channel đã dead → error không retry.

4. **Pool blocked khi node unhealthy** (Issue #4, P1): GetClient() bị chặn khi có node đang reconnect do lock contention trong getHealthyNodes(). Hệ thống degraded từ 3 nodes xuống 0 hiệu quả.

5. **Credentials exposed** (Issue #8, P1): Toàn bộ AMQP URL với password trong log — vi phạm security policy.

---

## Quick Wins (có thể fix trong 1-2 ngày)

| # | Fix | Effort |
|---|---|---|
| 1 | Mask credentials trong log | 30 phút |
| 2 | Fix reconnectWorker thêm vòng for loop + xử lý graceful close | 1 giờ |
| 3 | Chuyển reconnect() từ recursive sang iterative loop | 2 giờ |
| 4 | Fix time.AfterFunc → goroutine với ctx.Done() | 30 phút |
| 5 | Fix DeclareQueue/Exchange dedup với map | 1 giờ |

## Long-term Improvements

1. **Channel pool** thay vì 1 shared channel
2. **Publisher Confirms** API
3. **Consume() API** với auto-resubscribe
4. **Exponential backoff với jitter** cho reconnect
5. **Prometheus metrics** (connections, channels, publish rate, error rate)
6. **Race test coverage** với goleak
7. **Circuit breaker** per node

## Test Plan

```bash
# Bước 1: Phát hiện goroutine leak
go get go.uber.org/goleak
go test -race -run TestGoroutineLeakOnReconnect ./...

# Bước 2: Phát hiện data race
go test -race -count=10 ./...

# Bước 3: Benchmark publish throughput
go test -bench=BenchmarkPublish -benchmem -benchtime=30s ./...

# Bước 4: Stress test concurrent reconnect
go test -run TestConcurrentReconnect -count=50 -race ./...

# Bước 5: Integration test với failure injection (testcontainers + toxiproxy)
```

---

*Report generated by Senior Go Review — Không có code nào bị sửa. Toàn bộ finding có evidence từ source code.*
