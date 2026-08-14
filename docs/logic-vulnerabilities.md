# Báo cáo Lỗ hổng Logic (Logic Vulnerabilities) trong Bunnyhop

Qua quá trình rà soát mã nguồn dự án `bunnyhop` (đặc biệt là `pool.go` và `client.go`), hệ thống phát hiện một số lỗ hổng logic nghiêm trọng liên quan đến concurrency (xử lý đồng thời), quản lý kết nối và rò rỉ tài nguyên (resource leak). 

Dưới đây là chi tiết các lỗi và đề xuất khắc phục:

## 1. Rò rỉ Kết nối & Goroutine (Connection/Goroutine Leak)
- **Vị trí**: `client.go` - hàm `reconnect()` và `pool.go` - hàm `connectToNode()`.
- **Mô tả**: 
  - Khi mất kết nối, `Pool` tự động tạo một `Client` hoàn toàn mới (`client := NewClient(...)`) và đóng `Client` cũ.
  - Tuy nhiên, bên trong `Client` cũ, tiến trình `reconnect()` vẫn đang được thực thi ngầm. Hàm `time.Sleep(c.config.ReconnectInterval)` không thể bị hủy bằng `context`. Sau khi hết sleep, nó tiếp tục gọi `Connect(c.ctx)` mà không kiểm tra xem context đã bị hủy hay client đã bị đóng hay chưa (do thư viện `amqp.DialConfig` không nhận context).
- **Hậu quả**: Các `Client` cũ tưởng chừng đã bị đóng sẽ tiếp tục kết nối lại thành công tới RabbitMQ chạy ngầm, gây rò rỉ bộ nhớ (goroutine leak) và rò rỉ kết nối mạng (connection leak) vĩnh viễn.

## 2. Deadlock/Blocking toàn bộ Pool khi một Node lỗi
- **Vị trí**: `pool.go` - hàm `connectToNode()` và `getHealthyNodes()`.
- **Mô tả**:
  - `connectToNode()` giữ khóa độc quyền `node.mutex.Lock()` trong suốt quá trình `client.Connect(p.ctx)`. Quá trình tạo kết nối mạng này là đồng bộ (synchronous) và có thể mất nhiều giây nếu mạng bị nghẽn (timeout).
  - Cùng lúc đó, mỗi khi có request lấy client `GetClient()`, hàm `getHealthyNodes()` sẽ lặp qua toàn bộ các node và cố gắng lấy khóa đọc `node.mutex.RLock()`.
- **Hậu quả**: Khóa đọc sẽ bị block lại chờ khóa ghi đang bị giữ bởi `connectToNode()`. Hệ quả là nếu chỉ **một** node đang bị rớt mạng và cố kết nối lại, toàn bộ các request `GetClient()` sẽ bị treo (blocked), vô hiệu hóa hoàn toàn mục đích dự phòng của Pool.

## 3. Race Condition gây Reconnect lặp lại (Connection Churn)
- **Vị trí**: `pool.go` - hàm `watchNodeConnection()`, `checkNodeHealth()` và `connectToNode()`.
- **Mô tả**:
  - Cả hai luồng kiểm tra sức khỏe là `watchNodeConnection` và `healthCheckWorker` đều có thể phát hiện mất kết nối cùng một lúc và cùng đẩy một lệnh `go p.connectToNode(node)`.
  - Trong `connectToNode()`, biến cờ `node.connecting` được dùng để chống gọi trùng, nhưng lại bị reset ngay lập tức bằng `defer func() { node.connecting = false }()`.
- **Hậu quả**: Goroutine thứ 1 kết nối xong và nhả khóa. Goroutine thứ 2 (đang chờ khóa) sẽ nhảy vào, thấy `node.connecting = false` nên lại tiếp tục chạy logic kết nối. Nó sẽ đè và đóng kết nối hoàn toàn mới mà Goroutine 1 vừa cực nhọc tạo ra. Điều này gây nên chớp tắt kết nối liên tục không cần thiết.

## 4. Thiếu vòng lặp gây Mất kết nối ẩn (Silent Disconnect)
- **Vị trí**: `client.go` - hàm `reconnectWorker()`.
- **Mô tả**: 
  - Lệnh `select` dùng để bắt các sự kiện đóng kênh/kết nối nhưng **không nằm trong vòng lặp `for`**.
  - Hàm sẽ thoát vĩnh viễn ngay khi bắt được event đầu tiên. Nghiêm trọng hơn, nếu event trả về là graceful close (`err == nil`), nhánh `if ok && err != nil` bị bỏ qua và hàm kết thúc mà không hề gọi `handleDisconnection()`.
- **Hậu quả**: Client mất hoàn toàn khả năng nhận biết kết nối bị rớt trong tương lai, trừ phi có thao tác nào đó tác động trực tiếp và trigger lỗi.

## 5. Data Races (Tranh chấp dữ liệu)
- **Vị trí 1 - Metrics (`pool.go`)**: Biến `p.totalRequests` và `p.totalFailures` được tăng bằng `atomic.AddInt64(...)` trong hàm `GetClient()`, nhưng lại được đọc trực tiếp không qua atomic trong `GetStats()`. Đây là Data Race theo chuẩn Go.
- **Vị trí 2 - `Pool.Close()` (`pool.go`)**: Hàm này lặp qua các node và gọi `node.Client.Close()` mà không sử dụng `node.mutex.Lock()`. Có thể dẫn đến nil pointer hoặc panic nếu chạy song song với tiến trình gán `node.Client = client` bên trong `connectToNode()`.

---

## Tóm tắt Phương án xử lý:
1. **Sửa Client Leak**: Hủy tính năng tự reconnect trong `Client` (để `Pool` tự xử lý) HOẶC bổ sung check `c.ctx.Err()` chặt chẽ sau lệnh sleep trong hàm `reconnect()`. Nên đổi sang hàm `time.NewTimer` với `select` để ngắt ngay khi `ctx.Done()`.
2. **Sửa Blocking Pool**: Hàm `connectToNode()` chỉ nên giữ khóa khi cập nhật `node.Client` và các cờ trạng thái, **KHÔNG** giữ khóa trong lúc gọi I/O mạng `client.Connect()`.
3. **Sửa Connection Churn**: Logic `checkNodeHealth` không nên tự gọi `connectToNode` nếu đã có `watchNodeConnection` giám sát, hoặc thiết kế lại state-machine cho trạng thái node.
4. **Sửa reconnectWorker**: Đặt lệnh `select` vào trong vòng lặp `for`.
5. **Sửa Data Races**: Dùng `atomic.LoadInt64()` khi đọc metrics và thêm Lock thích hợp ở hàm `Pool.Close()`.
