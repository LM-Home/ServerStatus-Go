# Design Doc: ServerStatus-Go Refactor & Modernization

## 1. Goal
- Update Go version to 1.26.1.
- Modularize the codebase for better maintainability.
- Fix concurrency bugs and resource leaks.
- Optimize performance using modern Go features.

## 2. Architecture (Modularization)
The project will be split into the following packages:
- `pkg/config`: Parsing CLI flags and DSN.
- `pkg/common`: Shared types like `ServerStatus`, `MonitorServer`.
- `pkg/collector`: Logic for gathering OS-level metrics (CPU, RAM, Disk, Net).
- `pkg/monitor`: Ping (CU/CT/CM) and custom HTTP/TCP monitoring.
- `pkg/sender`: Managing TCP connection to the server, authentication, and data transmission.
- `cmd/client`: Entry point (main package).

## 3. Bug Fixes & Improvements
- **Worker Lifecycle**: Use `context.WithCancel` to ensure all monitoring goroutines are terminated when the connection restarts.
- **Throttling Fix**: Remove the logic that aggressively increases intervals (from 1s to 60s) in `pingWorker`. Use a constant interval or proper backoff only on errors.
- **State Management**: Replace inconsistent `sync.Map` and manual `Mutex` usage with a unified `Store` struct using `sync.RWMutex`.
- **Error Handling**: Use `log/slog` for structured logging. Ensure connections are closed properly on all error paths.

## 4. Optimizations
- **Object Reuse**: Use `sync.Pool` for `ServerStatus` objects and JSON buffers.
- **Asynchronous Sender**: Use a channel to buffer outgoing status updates so that slow network connections don't block collectors.
- **Modern Go**: Utilize `iter` package for range loops where applicable and `log/slog` for logging.

## 5. Testing Plan
- Unit tests for metric collection logic.
- Mock server to test connection/reconnection and auth flow.
