# Release Notes - Bunnyhop v1.0.5

**Ngày phát hành:** 14/08/2026  
**Phiên bản:** `v1.0.5`  
**Mục tiêu chính:** Nâng cấp độ ổn định cho môi trường Production High-Throughput / Cluster, vá toàn bộ các lỗ hổng bảo mật rò rỉ credential, khắc phục triệt để rò rỉ tài nguyên (goroutine & memory leak), loại bỏ nguy cơ deadlock và bổ sung Consumer API chuẩn.

---

## 🚀 Điểm nổi bật & Cải tiến quan trọng

### 1. Khắc phục sự cố Goroutine Leak & Silent Disconnect (P0)
- **Iterative Reconnect:** Chuyển đổi toàn bộ cơ chế `reconnect()` từ đệ quy goroutine (`go c.reconnect()`) sang vòng lặp `for` tuần tự có backoff và jitter. Loại bỏ hoàn toàn nguy cơ bùng nổ goroutine (goroutine storm) và crash do out-of-memory khi RabbitMQ downtime kéo dài.
- **Xử lý Graceful Close:** `reconnectWorker` hiện đã xử lý cả sự kiện connection/channel đóng bình thường (`ok=false`, `err=nil`), ngăn chặn tình trạng ngắt kết nối ngầm (silent disconnect).
- **Loại bỏ rò rỉ `time.AfterFunc`:** Thay thế `time.AfterFunc` trong pool reconnect bằng goroutine có kiểm soát qua `context.Context` (`p.ctx.Done()`), đảm bảo mọi tác vụ ngầm dừng ngay lập tức khi `Pool.Close()` được gọi.

### 2. Bảo mật & Giám sát (P0)
- **Masking Credential:** Bổ sung hàm `maskAMQPURL()` tự động ẩn mật khẩu (`***`) trong toàn bộ log của `Client` và `Pool`. Đảm bảo log tập trung (ELK, Loki, Datadog) không bao giờ lưu trữ plain-text password.

### 3. Tối ưu hóa Concurrency & Deadlock Prevention (P1)
- **Loại bỏ I/O trong Hot Path:** `getHealthyNodes()` trong `Pool` không còn thực hiện network I/O hay gọi `IsConnected()` bên trong `RWMutex`, giảm tối đa contention và tăng throughput khi gọi `GetClient()`.
- **Phòng chống Deadlock:** `Pool.GetStats()` sử dụng trạng thái cached thay vì gọi phương thức có nested lock.
- **Thundering Herd Protection:** Bổ sung Exponential Backoff với `±25%` random jitter cho chu kỳ thử kết nối lại (tối đa 5 phút).
- **Loại bỏ Goroutine trùng lặp:** Gỡ bỏ worker `watchNodeConnection` riêng rẽ, chuyển quyền quản trị trạng thái kết nối về duy nhất `healthCheckWorker`.
- **Ngăn ngừa Memory Leak:** Chuyển đổi lưu trữ `declaredQueues`, `declaredExchanges`, `boundQueues` từ `slice` sang `map` có cơ chế deduplication theo identifier, ngăn chặn việc khai báo lặp làm phình bộ nhớ.
- **Thread-safe Publisher:** Kiểm tra lại trạng thái kết nối và channel ngay bên trong lock `publishMutex` nhằm tránh race condition TOCTOU (Time-of-check to time-of-use).

### 4. Tính năng mới & Mở rộng API (P2)
- **Consumer API (`client.Consume`):** Cung cấp API tiêu chuẩn cho Consumer hỗ trợ tự động tái đăng ký (auto-resubscribe) sau khi kết nối mạng/cluster RabbitMQ được khôi phục.
- **Publisher Confirms (`PublishWithConfirm`):** Hỗ trợ publish message đồng bộ kèm timeout và chờ xác nhận (Ack/Nack) từ broker RabbitMQ.
- **Configurable QoS / Prefetch:** Bổ sung cấu hình `PrefetchCount` (mặc định 10), `PrefetchSize`, `PrefetchGlobal` cho cả `Config` và `PoolConfig`.
- **Fail-fast Config Validation:** Bổ sung method `Config.Validate()` kiểm tra tính hợp lệ của cấu hình trước khi khởi động.

---

## 📊 Danh sách thay đổi chi tiết

| Nhóm | Mô tả |
|---|---|
| **Security** | Mask toàn bộ password trong URL khi in log (`maskAMQPURL`) |
| **Bug Fix** | Chuyển `reconnect()` sang iterative loop, chống tràn goroutine |
| **Bug Fix** | Xử lý `NotifyClose` khi broker đóng graceful |
| **Bug Fix** | Re-read channel state trong `publishMutex` ở `PublishMessage` |
| **Bug Fix** | Map deduplication cho declared queues/exchanges/bindings |
| **Bug Fix** | Ngắt kết nối sạch và dọn dẹp goroutine khi gọi `Pool.Close()` / `Client.Close()` |
| **Feature** | Consumer API với auto-resubscription (`consumer.go`) |
| **Feature** | Exponential backoff + jitter cho reconnect |
| **Feature** | Publisher Confirm API (`PublishWithConfirm`) |
| **Feature** | Cấu hình QoS (`PrefetchCount`, `PrefetchSize`, `PrefetchGlobal`) |
| **Testing** | Thêm bộ unit test toàn diện cho mask URL, backoff, deduplication và config |

---

## 🧪 Kiểm thử chất lượng

- Unit Tests: `11/11 PASS`
- Static Analysis: `go vet ./...` (Clean)
- Race Detector: `go build -race ./...` (Clean)

---

## 📦 Hướng dẫn nâng cấp

Cập nhật version trong file `go.mod`:

```bash
go get -u github.com/VanDuc0209/bunnyhop@v1.0.5
```
