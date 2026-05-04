package sender

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"ServiceStatus/pkg/common"
	"ServiceStatus/pkg/config"
	"ServiceStatus/pkg/monitor"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Sender struct {
	cfg      *config.Config
	store    *common.Store
	monitor  *monitor.Monitor
	statusCh chan common.ServerStatus
}

func NewSender(cfg *config.Config, store *common.Store, mon *monitor.Monitor) *Sender {
	return &Sender{
		cfg:      cfg,
		store:    store,
		monitor:  mon,
		statusCh: make(chan common.ServerStatus, 10),
	}
}

func (s *Sender) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			s.connect(ctx)
			time.Sleep(3 * time.Second)
		}
	}
}

func (s *Sender) connect(ctx context.Context) {
	addr := net.JoinHostPort(s.cfg.Server, fmt.Sprintf("%d", s.cfg.Port))
	slog.Info("Connecting to server", "addr", addr)

	conn, err := net.DialTimeout("tcp", addr, 30*time.Second)
	if err != nil {
		slog.Error("Connect failed", "err", err)
		return
	}
	defer conn.Close()

	// 启用 TCP Keepalive，防止 Docker conntrack 清理空闲连接
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	if !s.handleAuth(conn) {
		return
	}

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	err = s.handleMonitorConfig(childCtx, conn)
	if err != nil {
		slog.Error("Handle monitor config failed", "err", err)
		return
	}

	// 后台读取：及时检测服务端断开连接
	go func() {
		buf := make([]byte, 512)
		for {
			_, err := conn.Read(buf)
			if err != nil {
				slog.Warn("Server connection read error, triggering reconnect", "err", err)
				cancel()
				return
			}
		}
	}()

	s.sendStatusLoop(childCtx, conn)
}

func (s *Sender) handleAuth(conn net.Conn) bool {
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil || !strings.Contains(string(buf[:n]), "Authentication required") {
		slog.Error("Auth requirement check failed", "err", err)
		return false
	}

	_, err = conn.Write([]byte(s.cfg.User + ":" + s.cfg.Password + "\n"))
	if err != nil {
		slog.Error("Send auth failed", "err", err)
		return false
	}

	n, err = conn.Read(buf)
	if err != nil || !strings.Contains(string(buf[:n]), "Authentication successful") {
		slog.Error("Auth failed", "response", string(buf[:n]), "err", err)
		return false
	}

	slog.Info("Authentication successful")
	return true
}

func (s *Sender) handleMonitorConfig(ctx context.Context, conn net.Conn) error {
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	data := string(buf[:n])

	if strings.Contains(data, "IPv4") {
		// checkIP = 6 (ignoring as we check both in background)
	} else if strings.Contains(data, "IPv6") {
		// checkIP = 4
	} else {
		return fmt.Errorf("unknown connection mode")
	}

	s.store.Update(func(st *common.Store) {
		// Stop existing custom monitors
		for _, ms := range st.MonitorServers {
			close(ms.Stop)
		}
		st.MonitorServers = make(map[string]*common.MonitorServer)
	})

	lines := strings.Split(data, "\n")
	for _, line := range lines {
		if strings.Contains(line, "monitor") && strings.Contains(line, "{") {
			start := strings.Index(line, "{")
			end := strings.LastIndex(line, "}") + 1
			if start == -1 || end <= start {
				continue
			}

			var cfg struct {
				Name     string `json:"name"`
				Type     string `json:"type"`
				Host     string `json:"host"`
				Interval int    `json:"interval"`
			}
			if err := json.Unmarshal([]byte(line[start:end]), &cfg); err != nil {
				continue
			}

			ms := &common.MonitorServer{
				Type:     cfg.Type,
				Host:     cfg.Host,
				Interval: cfg.Interval,
				Stop:     make(chan struct{}),
			}
			s.store.Update(func(st *common.Store) {
				st.MonitorServers[cfg.Name] = ms
			})
			go s.monitor.StartCustomMonitor(ctx, cfg.Name, ms)
		}
	}

	return nil
}

func (s *Sender) sendStatusLoop(ctx context.Context, conn net.Conn) {
	ticker := time.NewTicker(time.Duration(s.cfg.Interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			status := s.buildStatus()
			data, err := json.Marshal(status)
			if err != nil {
				slog.Error("Marshal status failed", "err", err)
				continue
			}

			slog.Debug("Sending update", "data", string(data))

			conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
			_, err = conn.Write([]byte("update " + string(data) + "\n"))
			if err != nil {
				slog.Error("Send status failed", "err", err)
				return
			}
		}
	}
}

func (s *Sender) buildStatus() common.ServerStatus {
	s.store.RLock()
	status := s.store
	s.store.RUnlock()

	custom := s.monitor.GetCustomMonitorData()

	return common.ServerStatus{
		Uptime:      status.Uptime,
		Load1:       jsoniter.Number(fmt.Sprintf("%.2f", status.Load1)),
		Load5:       jsoniter.Number(fmt.Sprintf("%.2f", status.Load5)),
		Load15:      jsoniter.Number(fmt.Sprintf("%.2f", status.Load15)),
		MemoryTotal: status.MemoryTotal,
		MemoryUsed:  status.MemoryUsed,
		SwapTotal:   status.SwapTotal,
		SwapUsed:    status.SwapUsed,
		HddTotal:    status.HddTotal,
		HddUsed:     status.HddUsed,
		CPU:         jsoniter.Number(fmt.Sprintf("%.1f", status.CPU)),
		NetworkRx:   status.NetworkRx,
		NetworkTx:   status.NetworkTx,
		NetworkIn:   status.NetworkIn,
		NetworkOut:  status.NetworkOut,
		Online4:     status.Online4,
		Online6:     status.Online6,
		PingCU:      status.PingCU,
		PingCM:      status.PingCM,
		PingCT:      status.PingCT,
		TimeCU:      status.TimeCU,
		TimeCT:      status.TimeCT,
		TimeCM:      status.TimeCM,
		TCP:         status.TCP,
		UDP:         status.UDP,
		Process:     status.Process,
		Thread:      status.Thread,
		IoRead:      status.IoRead,
		IoWrite:     status.IoWrite,
		Custom:      custom,
	}

}
