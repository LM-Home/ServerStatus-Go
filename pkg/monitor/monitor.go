package monitor

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"ServiceStatus/pkg/common"
	"ServiceStatus/pkg/config"
)

type Monitor struct {
	cfg   *config.Config
	store *common.Store
}

func NewMonitor(cfg *config.Config, store *common.Store) *Monitor {
	return &Monitor{cfg: cfg, store: store}
}

func (m *Monitor) Start(ctx context.Context) {
	go m.pingWorker(ctx, m.cfg.CU, "CU", m.cfg.ProbePort)
	go m.pingWorker(ctx, m.cfg.CT, "CT", m.cfg.ProbePort)
	go m.pingWorker(ctx, m.cfg.CM, "CM", m.cfg.ProbePort)
}

func (m *Monitor) pingWorker(ctx context.Context, host, mark string, port int) {
	const historyLen = 64
	history := make([]int, 0, historyLen)
	lostCount := 0
	interval := time.Duration(m.cfg.Interval) * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		ip, err := m.resolveIP(host)
		if err != nil {
			slog.Error("Ping resolve failed", "mark", mark, "host", host, "err", err)
			ip = host
		}

		if len(history) >= historyLen {
			if history[0] == 0 {
				lostCount--
			}
			history = history[1:]
		}

		start := time.Now()
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(ip, strconv.Itoa(port)), time.Second)
		if err != nil {
			lostCount++
			history = append(history, 0)
			m.store.Update(func(s *common.Store) {
				if mark == "CU" { s.TimeCU = 0 }
				if mark == "CM" { s.TimeCM = 0 }
				if mark == "CT" { s.TimeCT = 0 }
			})
		} else {
			conn.Close()
			delay := int(time.Since(start).Milliseconds())
			history = append(history, 1)
			m.store.Update(func(s *common.Store) {
				if mark == "CU" { s.TimeCU = delay }
				if mark == "CM" { s.TimeCM = delay }
				if mark == "CT" { s.TimeCT = delay }
			})
		}

		if len(history) > historyLen/2 {
			rate := float64(lostCount) / float64(len(history)) * 100
			m.store.Update(func(s *common.Store) {
				if mark == "CU" { s.PingCU = rate }
				if mark == "CM" { s.PingCM = rate }
				if mark == "CT" { s.PingCT = rate }
			})
		}

		time.Sleep(interval)
	}
}

func (m *Monitor) resolveIP(host string) (string, error) {
	if strings.Contains(host, ":") {
		return host, nil
	}
	prefer := strings.ToLower(m.cfg.ProbeProtocolPrefer)
	ipAddr, err := net.ResolveIPAddr(prefer, host)
	if err != nil {
		return "", err
	}
	return ipAddr.IP.String(), nil
}

func (m *Monitor) StartCustomMonitor(ctx context.Context, name string, ms *common.MonitorServer) {
	const historyLen = 64
	history := make([]int, 0, historyLen)
	lostCount := 0
	userInterval := time.Duration(ms.Interval) * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		case <-ms.Stop:
			return
		default:
		}

		if len(history) >= historyLen {
			if history[0] == 0 {
				lostCount--
			}
			history = history[1:]
		}

		success, dns, conn, down := m.monitorCheck(ms.Type, ms.Host)
		if success {
			history = append(history, 1)
			ms.DnsTime = dns
			ms.ConnectTime = conn
			ms.DownloadTime = down
		} else {
			lostCount++
			history = append(history, 0)
		}

		if len(history) > 5 {
			ms.OnlineRate = 1 - float64(lostCount)/float64(len(history))
		}

		time.Sleep(userInterval)
	}
}

func (m *Monitor) monitorCheck(protocol, host string) (bool, int, int, int) {
	switch protocol {
	case "http", "https":
		return m.monitorHTTP(protocol, host)
	case "tcp":
		return m.monitorTCP(host)
	default:
		return false, 0, 0, 0
	}
}

func (m *Monitor) monitorHTTP(protocol, host string) (bool, int, int, int) {
	address := strings.TrimPrefix(host, protocol+"://")
	port := 80
	if protocol == "https" { port = 443 }

	start := time.Now()
	ip, err := m.resolveIP(address)
	if err != nil { return false, 0, 0, 0 }
	dnsTime := int(time.Since(start).Milliseconds())

	start = time.Now()
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(ip, strconv.Itoa(port)), 6*time.Second)
	if err != nil { return false, dnsTime, 0, 0 }
	conn.Close()
	connectTime := int(time.Since(start).Milliseconds())

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	resp, err := client.Get(host)
	if err != nil { return false, dnsTime, connectTime, 0 }
	defer resp.Body.Close()

	code := resp.StatusCode
	if code >= 200 && code < 400 || code == 401 {
		return true, dnsTime, connectTime, int(time.Since(start).Milliseconds())
	}
	return false, dnsTime, connectTime, 0
}

func (m *Monitor) monitorTCP(host string) (bool, int, int, int) {
	parts := strings.Split(host, ":")
	if len(parts) != 2 { return false, 0, 0, 0 }
	
	start := time.Now()
	ip, err := m.resolveIP(parts[0])
	if err != nil { return false, 0, 0, 0 }
	dnsTime := int(time.Since(start).Milliseconds())

	start = time.Now()
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(ip, parts[1]), 6*time.Second)
	if err != nil { return false, dnsTime, 0, 0 }
	defer conn.Close()
	connectTime := int(time.Since(start).Milliseconds())

	start = time.Now()
	if _, err := conn.Write([]byte("GET / HTTP/1.2\r\n\r\n")); err != nil {
		return false, dnsTime, connectTime, 0
	}
	buf := make([]byte, 1024)
	conn.Read(buf)
	return true, dnsTime, connectTime, int(time.Since(start).Milliseconds())
}

func (m *Monitor) CheckNetwork(version int) bool {
	host := "ipv4.google.com"
	if version == 6 { host = "ipv6.google.com" }
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, "80"), 2*time.Second)
	if err != nil { return false }
	conn.Close()
	return true
}

func (m *Monitor) GetCustomMonitorData() string {
	m.store.RLock()
	defer m.store.RUnlock()

	var parts []string
	for name, ms := range m.store.MonitorServers {
		part := fmt.Sprintf("%s\\t解析: %d\\t连接: %d\\t下载: %d\\t在线率: <code>%.1f%%</code>",
			name, ms.DnsTime, ms.ConnectTime, ms.DownloadTime, ms.OnlineRate*100)
		parts = append(parts, part)
	}
	return strings.Join(parts, "<br>")
}
