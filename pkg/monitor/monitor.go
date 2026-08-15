package monitor

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
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

// msRoundUp 将耗时舍入到毫秒，并保证成功探测至少为 1ms，
// 避免 <1ms 的快速连接(如命中本机加速/代理)被 int 截断显示为 0ms。
func msRoundUp(d time.Duration) int {
	ms := d.Round(time.Millisecond).Milliseconds()
	if ms < 1 {
		ms = 1
	}
	return int(ms)
}

func (m *Monitor) Start(ctx context.Context) {
	go m.pingWorker(ctx, m.cfg.CU, "CU", m.cfg.ProbePort)
	go m.pingWorker(ctx, m.cfg.CT, "CT", m.cfg.ProbePort)
	go m.pingWorker(ctx, m.cfg.CM, "CM", m.cfg.ProbePort)
	go m.networkCheckWorker(ctx)
}

func (m *Monitor) networkCheckWorker(ctx context.Context) {
	ticker := time.NewTicker(150 * time.Second)
	defer ticker.Stop()

	check := func() {
		o4 := m.CheckNetwork(4)
		o6 := m.CheckNetwork(6)
		m.store.Update(func(s *common.Store) {
			s.Online4 = o4
			s.Online6 = o6
		})
	}

	check() // Initial check
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			check()
		}
	}
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
			delay := msRoundUp(time.Since(start))
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
	start := time.Now()
	if _, err := m.resolveIP(address); err != nil {
		return false, 0, 0, 0
	}
	dnsTime := msRoundUp(time.Since(start))

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	start = time.Now()
	resp, err := client.Get(host)
	if err != nil { return false, dnsTime, 0, 0 }
	defer resp.Body.Close()
	roundTrip := msRoundUp(time.Since(start))

	code := resp.StatusCode
	if code >= 200 && code < 400 || code == 401 {
		return true, dnsTime, roundTrip, roundTrip
	}
	return false, dnsTime, roundTrip, 0
}

func (m *Monitor) monitorTCP(host string) (bool, int, int, int) {
	parts := strings.Split(host, ":")
	if len(parts) != 2 { return false, 0, 0, 0 }
	
	start := time.Now()
	ip, err := m.resolveIP(parts[0])
	if err != nil { return false, 0, 0, 0 }
	dnsTime := msRoundUp(time.Since(start))

	start = time.Now()
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(ip, parts[1]), 6*time.Second)
	if err != nil { return false, dnsTime, 0, 0 }
	defer conn.Close()
	connectTime := msRoundUp(time.Since(start))

	start = time.Now()
	if _, err := conn.Write([]byte("GET / HTTP/1.2\r\n\r\n")); err != nil {
		return false, dnsTime, connectTime, 0
	}
	buf := make([]byte, 1024)
	conn.Read(buf)
	return true, dnsTime, connectTime, msRoundUp(time.Since(start))
}

func (m *Monitor) CheckNetwork(version int) bool {
	host := "captive.apple.com"
	network := "tcp4"
	if version == 6 {
		network = "tcp6"
	}
	// 强制指定 IP 协议栈（tcp4/tcp6），避免 net.Dial 对
	// 同时存在 A/AAAA 记录的域名做 happy-eyeballs 回退到另一栈，
	// 导致仅有单栈的机器被误判为双栈。
	conn, err := net.DialTimeout(network, net.JoinHostPort(host, "80"), 2*time.Second)
	if err != nil { return false }
	conn.Close()
	return true
}

func (m *Monitor) GetCustomMonitorData() string {
	m.store.RLock()
	defer m.store.RUnlock()

	type MonItem struct {
		Name string  `json:"name"`
		Dns  int     `json:"dns"`
		Conn int     `json:"conn"`
		Down int     `json:"down"`
		Rate float64 `json:"rate"`
	}

	var items []MonItem
	for name, ms := range m.store.MonitorServers {
		items = append(items, MonItem{
			Name: name,
			Dns:  ms.DnsTime,
			Conn: ms.ConnectTime,
			Down: ms.DownloadTime,
			Rate: ms.OnlineRate * 100,
		})
	}

	if len(items) == 0 {
		return ""
	}

	// 上游服务端前端(web/js/app.js parseCustom)只解析 name=ms;name=ms 格式，
	// 与 Python 客户端 array['custom'] = ';'.join(f"{k}={v}") 保持一致。
	// 使用连接往返耗时(ConnectTime)作为探测延迟；未成功探测时为 0。
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })

	parts := make([]string, 0, len(items))
	for _, it := range items {
		latency := it.Conn
		if latency < 0 {
			latency = 0
		}
		parts = append(parts, fmt.Sprintf("%s=%d", it.Name, latency))
	}
	return strings.Join(parts, ";")
}
