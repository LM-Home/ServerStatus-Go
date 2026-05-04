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
	cfg     *config.Config
	store   *common.Store
	monitor *monitor.Monitor
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
			if start == -1 || end <= start { continue }

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
				Type: cfg.Type,
				Host: cfg.Host,
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
	defer s.store.RUnlock()

	return common.ServerStatus{
		Uptime:      s.store.Uptime,
		Load1:       jsoniter.Number(fmt.Sprintf("%.2f", s.store.Load1)),
		Load5:       jsoniter.Number(fmt.Sprintf("%.2f", s.store.Load5)),
		Load15:      jsoniter.Number(fmt.Sprintf("%.2f", s.store.Load15)),
		MemoryTotal: s.store.MemoryTotal,
		MemoryUsed:  s.store.MemoryUsed,
		SwapTotal:   s.store.SwapTotal,
		SwapUsed:    s.store.SwapUsed,
		HddTotal:    s.store.HddTotal,
		HddUsed:     s.store.HddUsed,
		CPU:         jsoniter.Number(fmt.Sprintf("%.1f", s.store.CPU)),
		NetworkRx:   s.store.NetworkRx,
		NetworkTx:   s.store.NetworkTx,
		NetworkIn:   s.store.NetworkIn,
		NetworkOut:  s.store.NetworkOut,
		Online4:     s.store.Online4,
		Online6:     s.store.Online6,
		PingCU:      s.store.PingCU,
		PingCM:      s.store.PingCM,
		PingCT:      s.store.PingCT,
		TimeCU:      s.store.TimeCU,
		TimeCT:      s.store.TimeCT,
		TimeCM:      s.store.TimeCM,
		TCP:         s.store.TCP,
		UDP:         s.store.UDP,
		Process:     s.store.Process,
		Thread:      s.store.Thread,
		IoRead:      s.store.IoRead,
		IoWrite:     s.store.IoWrite,
		Custom:      s.monitor.GetCustomMonitorData(),
	}
}
