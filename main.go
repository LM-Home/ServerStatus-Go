package main

import (
	"bufio"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
)

var (
	SERVER                 = flag.String("h", "127.0.0.1", "Server host")
	PORT                   = flag.Int("port", 35601, "Server port")
	USER                   = flag.String("u", "s01", "Client username")
	PASSWORD               = flag.String("p", "USER_DEFAULT_PASSWORD", "Client password")
	INTERVAL               = flag.Float64("interval", 1.0, "Monitoring interval (seconds)")
	DSN                    = flag.String("dsn", "", "DSN format: username:password@host:port")
	isVnstat               = flag.Bool("vnstat", false, "Use vnstat for traffic (linux only)")
	CU                     = flag.String("cu", "cu.tz.cloudcpp.com", "CU probe host")
	CT                     = flag.String("ct", "ct.tz.cloudcpp.com", "CT probe host")
	CM                     = flag.String("cm", "cm.tz.cloudcpp.com", "CM probe host")
	PROBEPORT              = flag.Int("probeport", 80, "Probe port")
	PROBE_PROTOCOL_PREFER  = flag.String("proto", "ipv4", "Protocol preference (ipv4/ipv6)")
	PING_PACKET_HISTORY_LEN = 100
	ONLINE_PACKET_HISTORY_LEN = 72
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// 全局状态存储（带并发保护）
var (
	lostRate     = sync.Map{} // key: mark(string), value: float64(%)
	pingTime     = sync.Map{} // key: mark(string), value: int(ms)
	netSpeed     = struct {
		sync.Mutex
		netrx  int64
		nettx  int64
		clock  float64
		diff   float64
		avgrx  int64
		avgtx  int64
	}{}
	diskIO = struct {
		sync.Mutex
		read  int64
		write int64
	}{}
	monitorServer = struct {
		sync.RWMutex
		servers map[string]*MonitorServer
	}{
		servers: make(map[string]*MonitorServer),
	}
)

// MonitorServer 自定义服务器监控数据
type MonitorServer struct {
	Type         string  `json:"type"`
	DnsTime      int     `json:"dns_time"`
	ConnectTime  int     `json:"connect_time"`
	DownloadTime int     `json:"download_time"`
	OnlineRate   float64 `json:"online_rate"`
	host         string
	interval     int
	stop         chan struct{}
}

// ServerStatus 完整状态数据结构
type ServerStatus struct {
	Uptime       uint64          `json:"uptime"`
	Load1        jsoniter.Number `json:"load_1"`
	Load5        jsoniter.Number `json:"load_5"`
	Load15       jsoniter.Number `json:"load_15"`
	MemoryTotal  uint64          `json:"memory_total"`
	MemoryUsed   uint64          `json:"memory_used"`
	SwapTotal    uint64          `json:"swap_total"`
	SwapUsed     uint64          `json:"swap_used"`
	HddTotal     uint64          `json:"hdd_total"`
	HddUsed      uint64          `json:"hdd_used"`
	CPU          jsoniter.Number `json:"cpu"`
	NetworkRx    int64           `json:"network_rx"`
	NetworkTx    int64           `json:"network_tx"`
	NetworkIn    uint64          `json:"network_in"`
	NetworkOut   uint64          `json:"network_out"`
	Online4      bool            `json:"online4,omitempty"`
	Online6      bool            `json:"online6,omitempty"`
	Ping10010    float64         `json:"ping_10010"`
	Ping189      float64         `json:"ping_189"`
	Ping10086    float64         `json:"ping_10086"`
	Time10010    int             `json:"time_10010"`
	Time189      int             `json:"time_189"`
	Time10086    int             `json:"time_10086"`
	TCP          int             `json:"tcp"`
	UDP          int             `json:"udp"`
	Process      int             `json:"process"`
	Thread       int             `json:"thread"`
	IoRead       int64           `json:"io_read"`
	IoWrite      int64           `json:"io_write"`
	Custom       string          `json:"custom"`
}

func main() {
	flag.Parse()
	parseDSN()
	validateParams()

	// 启动所有监控线程
	startBackgroundMonitors()

	// 主连接循环
	for {
		connect()
		time.Sleep(3 * time.Second)
	}
}

// 解析DSN参数
func parseDSN() {
	if *DSN != "" {
		parts := strings.Split(*DSN, "@")
		if len(parts) != 2 {
			log.Fatal("Invalid DSN format")
		}
		auth := strings.Split(parts[0], ":")
		if len(auth) != 2 {
			log.Fatal("Invalid DSN auth part")
		}
		*USER = auth[0]
		*PASSWORD = auth[1]

		addr := strings.Split(parts[1], ":")
		*SERVER = addr[0]
		if len(addr) == 2 {
			port, err := strconv.Atoi(addr[1])
			if err == nil {
				*PORT = port
			}
		}
	}
}

// 验证参数有效性
func validateParams() {
	if *PORT < 1 || *PORT > 65535 {
		log.Fatal("Invalid port number")
	}
	if *SERVER == "" || *USER == "" || *PASSWORD == "" {
		log.Fatal("SERVER, USER and PASSWORD must be provided")
	}
	if *PROBE_PROTOCOL_PREFER != "ipv4" && *PROBE_PROTOCOL_PREFER != "ipv6" {
		log.Fatal("Protocol preference must be ipv4 or ipv6")
	}
}

// 启动所有后台监控线程
func startBackgroundMonitors() {
	// 启动多目标Ping监测
	go pingWorker(*CU, "10010", *PROBEPORT)
	go pingWorker(*CT, "189", *PROBEPORT)
	go pingWorker(*CM, "10086", *PROBEPORT)

	// 启动网络速率监测
	go netSpeedMonitor()

	// 启动磁盘IO监测
	go diskIOMonitor()
}

// pingWorker 多目标Ping监测工作线程
func pingWorker(host, mark string, port int) {
	lostCount := 0
	history := make([]int, 0, PING_PACKET_HISTORY_LEN)
	interval := time.Duration(*INTERVAL) * time.Second

	for {
		// 解析IP（优先指定协议）
		ip, err := resolveIP(host)
		if err != nil {
			ip = host // 解析失败直接使用主机名
		}

		// 维护历史队列
		if len(history) >= PING_PACKET_HISTORY_LEN {
			if history[0] == 0 {
				lostCount--
			}
			history = history[1:]
		}

		// 执行连接测试
		start := time.Now()
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(ip, strconv.Itoa(port)), time.Second)
		if err != nil {
			lostCount++
			history = append(history, 0)
			pingTime.Store(mark, 0) // 超时记为0
		} else {
			conn.Close()
			delay := int(time.Since(start).Milliseconds())
			pingTime.Store(mark, delay)
			history = append(history, 1)
		}

		// 计算丢包率
		if len(history) > 30 {
			rate := float64(lostCount) / float64(len(history)) * 100
			lostRate.Store(mark, rate)
		}

		time.Sleep(interval)
	}
}

// resolveIP 根据协议偏好解析IP
func resolveIP(host string) (string, error) {
	if strings.Contains(host, ":") {
		return host, nil // 已为IPv6地址
	}

	addrType := net.ResolveIPAddr
	if *PROBE_PROTOCOL_PREFER == "ipv6" {
		addrType = func(network, address string) (*net.IPAddr, error) {
			return net.ResolveIPAddr("ip6", address)
		}
	} else {
		addrType = func(_, address string) (*net.IPAddr, error) {
			return net.ResolveIPAddr("ip4", address)
		}
	}

	ipAddr, err := addrType("", host)
	if err != nil {
		return "", err
	}
	return ipAddr.IP.String(), nil
}

// netSpeedMonitor 网络速率监测
func netSpeedMonitor() {
	interval := time.Duration(*INTERVAL) * time.Second
	netSpeed.avgrx = 0
	netSpeed.avgtx = 0
	netSpeed.clock = float64(time.Now().UnixNano()) / 1e9

	for {
		avgrx, avgtx, err := getNetBytes()
		if err != nil {
			log.Println("Net speed error:", err)
			time.Sleep(interval)
			continue
		}

		now := float64(time.Now().UnixNano()) / 1e9
		netSpeed.Lock()
		netSpeed.diff = now - netSpeed.clock
		if netSpeed.diff > 0 {
			netSpeed.netrx = int64(float64(avgrx-netSpeed.avgrx) / netSpeed.diff)
			netSpeed.nettx = int64(float64(avgtx-netSpeed.avgtx) / netSpeed.diff)
		}
		netSpeed.clock = now
		netSpeed.avgrx = avgrx
		netSpeed.avgtx = avgtx
		netSpeed.Unlock()

		time.Sleep(interval)
	}
}

// getNetBytes 获取非虚拟网卡的累计字节数
func getNetBytes() (rx, tx int64, err error) {
	file, err := os.Open("/proc/net/dev")
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// 跳过前两行标题
	scanner.Scan()
	scanner.Scan()

	virtRegex := regexp.MustCompile(`lo|tun|docker|veth|br-|vmbr|vnet|kube`)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) < 10 {
			continue
		}
		dev := strings.TrimSuffix(parts[0], ":")
		if virtRegex.MatchString(dev) {
			continue
		}

		rxBytes, _ := strconv.ParseInt(parts[1], 10, 64)
		txBytes, _ := strconv.ParseInt(parts[9], 10, 64)
		rx += rxBytes
		tx += txBytes
	}

	return rx, tx, scanner.Err()
}

// diskIOMonitor 磁盘IO监测
func diskIOMonitor() {
	interval := time.Duration(*INTERVAL) * time.Second
	excludeProcs := map[string]bool{"bash": true}

	for {
		// 第一次采样
		first, err := getProcessIO()
		if err != nil {
			log.Println("Disk IO first sample error:", err)
			time.Sleep(interval)
			continue
		}

		time.Sleep(interval)

		// 第二次采样
		second, err := getProcessIO()
		if err != nil {
			log.Println("Disk IO second sample error:", err)
			time.Sleep(interval)
			continue
		}

		// 计算差值
		var read, write int64
		for pid, io1 := range first {
			io2, ok := second[pid]
			if !ok || io1.Name != io2.Name {
				continue
			}
			if excludeProcs[io1.Name] {
				continue
			}
			read += io2.Read - io1.Read
			write += io2.Write - io1.Write
		}

		diskIO.Lock()
		diskIO.read = read
		diskIO.write = write
		diskIO.Unlock()
	}
}

// 进程IO信息
type procIO struct {
	Name  string
	Read  int64
	Write int64
}

// getProcessIO 获取所有进程的IO信息
func getProcessIO() (map[string]procIO, error) {
	pids, err := filepath.Glob("/proc/[0-9]*")
	if err != nil {
		return nil, err
	}

	result := make(map[string]procIO)
	for _, pidPath := range pids {
		pid := filepath.Base(pidPath)
		ioPath := filepath.Join(pidPath, "io")
		commPath := filepath.Join(pidPath, "comm")

		// 读取进程名
		comm, err := os.ReadFile(commPath)
		if err != nil {
			continue
		}
		name := strings.TrimSpace(string(comm))

		// 读取IO信息
		ioData, err := os.ReadFile(ioPath)
		if err != nil {
			continue
		}

		// 解析read_bytes和write_bytes
		var read, write int64
		lines := strings.Split(string(ioData), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "read_bytes:") {
				read, _ = strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "read_bytes:")), 10, 64)
			} else if strings.HasPrefix(line, "write_bytes:") && !strings.Contains(line, "cancelled") {
				write, _ = strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "write_bytes:")), 10, 64)
			}
		}

		result[pid] = procIO{Name: name, Read: read, Write: write}
	}

	return result, nil
}

// 连接服务器并发送状态数据
func connect() {
	log.Println("Connecting to", *SERVER, *PORT)
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(*SERVER, strconv.Itoa(*PORT)), 30*time.Second)
	if err != nil {
		log.Println("Connection failed:", err)
		return
	}
	defer conn.Close()

	// 处理认证
	if !handleAuth(conn) {
		return
	}

	// 处理监控配置
	checkIP, err := handleMonitorConfig(conn)
	if err != nil {
		log.Println("Handle config error:", err)
		return
	}

	// 发送状态数据循环
	sendStatusLoop(conn, checkIP)
}

// 处理认证流程
func handleAuth(conn net.Conn) bool {
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil || !strings.Contains(string(buf[:n]), "Authentication required") {
		log.Println("Auth required check failed:", err)
		return false
	}

	// 发送认证信息
	_, err = conn.Write([]byte(*USER + ":" + *PASSWORD + "\n"))
	if err != nil {
		log.Println("Send auth failed:", err)
		return false
	}

	// 验证认证结果
	n, err = conn.Read(buf)
	if err != nil || !strings.Contains(string(buf[:n]), "Authentication successful") {
		log.Println("Auth failed:", string(buf[:n]), err)
		return false
	}

	return true
}

// 处理监控配置并返回需要检查的IP版本
func handleMonitorConfig(conn net.Conn) (int, error) {
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return 0, err
	}
	data := string(buf[:n])

	// 确定需要检查的IP版本
	checkIP := 0
	if strings.Contains(data, "IPv4") {
		checkIP = 6
	} else if strings.Contains(data, "IPv6") {
		checkIP = 4
	} else {
		return 0, fmt.Errorf("unknown connection type")
	}

	// 解析监控服务器配置
	monitorServer.Lock()
	defer monitorServer.Unlock()
	monitorServer.servers = make(map[string]*MonitorServer) // 重置

	lines := strings.Split(data, "\n")
	for _, line := range lines {
		if strings.Contains(line, "monitor") && strings.Contains(line, "type") && strings.Contains(line, "{") && strings.Contains(line, "}") {
			start := strings.Index(line, "{")
			end := strings.LastIndex(line, "}") + 1
			if start == -1 || end == 0 {
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

			ms := &MonitorServer{
				Type:     cfg.Type,
				host:     cfg.Host,
				interval: cfg.Interval,
				stop:     make(chan struct{}),
			}
			monitorServer.servers[cfg.Name] = ms
			go monitorWorker(cfg.Name, ms)
		}
	}

	return checkIP, nil
}

// monitorWorker 自定义服务器监控工作线程
func monitorWorker(name string, ms *MonitorServer) {
	lostCount := 0
	history := make([]int, 0, ONLINE_PACKET_HISTORY_LEN)
	interval := time.Duration(ms.interval) * time.Second

	for {
		select {
		case <-ms.stop:
			return
		default:
		}

		// 检查服务器是否仍在监控列表中
		monitorServer.RLock()
		_, exists := monitorServer.servers[name]
		monitorServer.RUnlock()
		if !exists {
			return
		}

		// 维护历史队列
		if len(history) >= ONLINE_PACKET_HISTORY_LEN {
			if history[0] == 0 {
				lostCount--
			}
			history = history[1:]
		}

		// 执行监控检查
		success, dnsTime, connectTime, downloadTime := monitorCheck(ms.Type, ms.host)
		if success {
			history = append(history, 1)
			ms.DnsTime = dnsTime
			ms.ConnectTime = connectTime
			ms.DownloadTime = downloadTime
		} else {
			lostCount++
			history = append(history, 0)
		}

		// 计算在线率
		if len(history) > 5 {
			ms.OnlineRate = 1 - float64(lostCount)/float64(len(history))
		}

		time.Sleep(interval)
	}
}

// monitorCheck 执行具体协议的监控检查
func monitorCheck(protocol, host string) (success bool, dnsTime, connectTime, downloadTime int) {
	switch protocol {
	case "http", "https":
		return monitorHTTP(protocol, host)
	case "tcp":
		return monitorTCP(host)
	default:
		return false, 0, 0, 0
	}
}

// monitorHTTP HTTP/HTTPS监控
func monitorHTTP(protocol, host string) (success bool, dnsTime, connectTime, downloadTime int) {
	address := strings.TrimPrefix(host, protocol+"://")
	port := 80
	if protocol == "https" {
		port = 443
	}

	// DNS解析时间
	start := time.Now()
	ip, err := resolveIP(address)
	if err != nil {
		return false, 0, 0, 0
	}
	dnsTime = int(time.Since(start).Milliseconds())

	// 连接时间
	start = time.Now()
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(ip, strconv.Itoa(port)), 6*time.Second)
	if err != nil {
		return false, dnsTime, 0, 0
	}
	defer conn.Close()
	connectTime = int(time.Since(start).Milliseconds())

	// HTTPS握手
	var tlsConn *tls.Conn
	if protocol == "https" {
		startTLS := time.Now()
		tlsConn = tls.Client(conn, &tls.Config{
			ServerName: address,
			InsecureSkipVerify: true, // 跳过证书验证，与Python版本保持一致
		})
		if err := tlsConn.Handshake(); err != nil {
			return false, dnsTime, connectTime, 0
		}
		connectTime += int(time.Since(startTLS).Milliseconds())
	}

	// 下载时间
	start = time.Now()
	req := fmt.Sprintf("GET / HTTP/1.2\r\nHost:%s\r\nUser-Agent:ServerStatus/goclient\r\nConnection:close\r\n\r\n", address)
	var writer io.Writer = conn
	if protocol == "https" {
		writer = tlsConn
	}
	if _, err := writer.Write([]byte(req)); err != nil {
		return false, dnsTime, connectTime, 0
	}

	// 读取响应
	buf := make([]byte, 4096)
	_, err = (io.Reader(conn)).Read(buf)
	if err != nil && err != io.EOF {
		return false, dnsTime, connectTime, 0
	}

	// 验证状态码
	resp := string(buf)
	statusLine := strings.Split(resp, "\r\n")[0]
	statusParts := strings.Split(statusLine, " ")
	if len(statusParts) < 2 {
		return false, dnsTime, connectTime, 0
	}
	code := statusParts[1]
	validCodes := map[string]bool{"200": true, "204": true, "301": true, "302": true, "401": true}
	if !validCodes[code] {
		return false, dnsTime, connectTime, 0
	}

	downloadTime = int(time.Since(start).Milliseconds())
	return true, dnsTime, connectTime, downloadTime
}

// monitorTCP TCP监控
func monitorTCP(host string) (success bool, dnsTime, connectTime, downloadTime int) {
	parts := strings.Split(host, ":")
	if len(parts) != 2 {
		return false, 0, 0, 0
	}
	address, portStr := parts[0], parts[1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return false, 0, 0, 0
	}

	// DNS解析时间
	start := time.Now()
	ip, err := resolveIP(address)
	if err != nil {
		return false, 0, 0, 0
	}
	dnsTime = int(time.Since(start).Milliseconds())

	// 连接时间
	start = time.Now()
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(ip, strconv.Itoa(port)), 6*time.Second)
	if err != nil {
		return false, dnsTime, 0, 0
	}
	defer conn.Close()
	connectTime = int(time.Since(start).Milliseconds())

	// 下载时间
	start = time.Now()
	if _, err := conn.Write([]byte("GET / HTTP/1.2\r\n\r\n")); err != nil {
		return false, dnsTime, connectTime, 0
	}
	buf := make([]byte, 1024)
	if _, err := conn.Read(buf); err != nil && err != io.EOF {
		return false, dnsTime, connectTime, 0
	}
	downloadTime = int(time.Since(start).Milliseconds())

	return true, dnsTime, connectTime, downloadTime
}

// 发送状态数据循环
func sendStatusLoop(conn net.Conn, checkIP int) {
	timer := 0.0
	interval := time.Duration(*INTERVAL) * time.Second

	for {
		// 收集系统状态数据
		status := collectStatus(checkIP, &timer)

		// 序列化并发送
		data, err := json.Marshal(status)
		if err != nil {
			log.Println("Marshal error:", err)
			break
		}

		_, err = conn.Write([]byte("update " + string(data) + "\n"))
		if err != nil {
			log.Println("Write error:", err)
			break
		}

		time.Sleep(interval)
	}
}

// 收集系统状态数据
func collectStatus(checkIP int, timer *float64) ServerStatus {
	// CPU使用率
	cpu := getCPU()

	// 网络流量
	var netIn, netOut uint64
	var err error
	if *isVnstat {
		netIn, netOut, err = trafficVnstat()
		if err != nil {
			log.Println("Vnstat error:", err)
		}
	} else {
		rx, tx, _ := getNetBytes()
		netIn, netOut = uint64(rx), uint64(tx)
	}

	// 网络速率
	netSpeed.Lock()
	netRx, netTx := netSpeed.netrx, netSpeed.nettx
	netSpeed.Unlock()

	// 内存信息
	memTotal, memUsed, swapTotal, swapFree := getMemory()

	// 磁盘信息
	hddTotal, hddUsed := getDisk()

	// 系统负载
	load1, load5, load15 := getLoad()

	// 在线状态检查
	var online4, online6 bool
	if *timer <= 0 {
		if checkIP == 4 {
			online4 = checkNetwork(4)
		} else {
			online6 = checkNetwork(6)
		}
		*timer = 150.0 // 每150秒检查一次
	}
	*timer -= *INTERVAL

	// Ping数据
	ping10010, _ := lostRate.Load("10010")
	ping189, _ := lostRate.Load("189")
	ping10086, _ := lostRate.Load("10086")
	time10010, _ := pingTime.Load("10010")
	time189, _ := pingTime.Load("189")
	time10086, _ := pingTime.Load("10086")

	// 连接数和进程数
	tcp, udp, process, thread := getTupd()

	// 磁盘IO
	diskIO.Lock()
	ioRead, ioWrite := diskIO.read, diskIO.write
	diskIO.Unlock()

	// 自定义监控数据
	custom := getCustomMonitorData()

	return ServerStatus{
		Uptime:       getUptime(),
		Load1:        jsoniter.Number(fmt.Sprintf("%.2f", load1)),
		Load5:        jsoniter.Number(fmt.Sprintf("%.2f", load5)),
		Load15:       jsoniter.Number(fmt.Sprintf("%.2f", load15)),
		MemoryTotal:  memTotal,
		MemoryUsed:   memUsed,
		SwapTotal:    swapTotal,
		SwapUsed:     swapTotal - swapFree,
		HddTotal:     hddTotal,
		HddUsed:      hddUsed,
		CPU:          jsoniter.Number(fmt.Sprintf("%.1f", cpu)),
		NetworkRx:    netRx,
		NetworkTx:    netTx,
		NetworkIn:    netIn,
		NetworkOut:   netOut,
		Online4:      online4,
		Online6:      online6,
		Ping10010:    ping10010.(float64),
		Ping189:      ping189.(float64),
		Ping10086:    ping10086.(float64),
		Time10010:    time10010.(int),
		Time189:      time189.(int),
		Time10086:    time10086.(int),
		TCP:          tcp,
		UDP:          udp,
		Process:      process,
		Thread:       thread,
		IoRead:       ioRead,
		IoWrite:      ioWrite,
		Custom:       custom,
	}
}

// 系统信息收集函数（底层实现）
func getUptime() uint64 {
	data, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	parts := strings.Split(string(data), ".")
	uptime, _ := strconv.ParseUint(parts[0], 10, 64)
	return uptime
}

func getMemory() (total, used, swapTotal, swapFree uint64) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, 0, 0
	}

	memInfo := make(map[string]uint64)
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			val, _ := strconv.ParseUint(parts[1], 10, 64)
			memInfo[parts[0]] = val
		}
	}

	total = memInfo["MemTotal:"]
	free := memInfo["MemFree:"]
	buffers := memInfo["Buffers:"]
	cached := memInfo["Cached:"]
	sreclaimable := memInfo["SReclaimable:"]
	used = total - free - buffers - cached - sreclaimable

	swapTotal = memInfo["SwapTotal:"]
	swapFree = memInfo["SwapFree:"]

	return total, used, swapTotal, swapFree
}

func getDisk() (total, used uint64) {
	cmd := exec.Command("df", "-Tlm", "--total", "-t", "ext4", "-t", "ext3", "-t", "ext2", "-t", "reiserfs", "-t", "jfs", "-t", "ntfs", "-t", "fat32", "-t", "btrfs", "-t", "fuseblk", "-t", "zfs", "-t", "simfs", "-t", "xfs")
	output, err := cmd.Output()
	if err != nil {
		return 0, 0
	}

	lines := strings.Split(string(output), "\n")
	if len(lines) < 1 {
		return 0, 0
	}
	lastLine := lines[len(lines)-2] // 最后一行是总计
	parts := strings.Fields(lastLine)
	if len(parts) >= 4 {
		total, _ = strconv.ParseUint(parts[2], 10, 64)
		used, _ = strconv.ParseUint(parts[3], 10, 64)
	}
	return total, used
}

func getCPU() float64 {
	// 读取初始CPU时间
	start, err := getCPUTime()
	if err != nil {
		return 0
	}
	time.Sleep(time.Duration(*INTERVAL) * time.Second)

	// 读取结束CPU时间
	end, err := getCPUTime()
	if err != nil {
		return 0
	}

	// 计算总时间和空闲时间差值
	total := end.user + end.nice + end.system + end.idle - (start.user + start.nice + start.system + start.idle)
	idle := end.idle - start.idle

	if total == 0 {
		return 0
	}
	return 100 - (float64(idle) / float64(total) * 100)
}

type cpuTime struct {
	user, nice, system, idle uint64
}

func getCPUTime() (cpuTime, error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return cpuTime{}, err
	}

	parts := strings.Fields(string(data))
	if len(parts) < 5 {
		return cpuTime{}, fmt.Errorf("invalid cpu stat")
	}

	user, _ := strconv.ParseUint(parts[1], 10, 64)
	nice, _ := strconv.ParseUint(parts[2], 10, 64)
	system, _ := strconv.ParseUint(parts[3], 10, 64)
	idle, _ := strconv.ParseUint(parts[4], 10, 64)

	return cpuTime{user, nice, system, idle}, nil
}

func getLoad() (load1, load5, load15 float64) {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, 0, 0
	}

	parts := strings.Fields(string(data))
	if len(parts) >= 3 {
		load1, _ = strconv.ParseFloat(parts[0], 64)
		load5, _ = strconv.ParseFloat(parts[1], 64)
		load15, _ = strconv.ParseFloat(parts[2], 64)
	}
	return load1, load5, load15
}

func checkNetwork(version int) bool {
	host := "ipv4.google.com"
	if version == 6 {
		host = "ipv6.google.com"
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, "80"), 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func trafficVnstat() (in, out uint64, err error) {
	cmd := exec.Command("vnstat", "--dumpdb", "1")
	output, err := cmd.Output()
	if err != nil {
		return 0, 0, err
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "totalrx;") {
			parts := strings.Split(line, ";")
			if len(parts) >= 2 {
				in, _ = strconv.ParseUint(parts[1], 10, 64)
			}
		} else if strings.HasPrefix(line, "totaltx;") {
			parts := strings.Split(line, ";")
			if len(parts) >= 2 {
				out, _ = strconv.ParseUint(parts[1], 10, 64)
			}
		}
	}
	return in, out, nil
}

func getTupd() (tcp, udp, process, thread int) {
	// TCP连接数
	tcpOut, _ := exec.Command("sh", "-c", "ss -t | wc -l").Output()
	tcp, _ = strconv.Atoi(strings.TrimSpace(string(tcpOut)))
	tcp = max(tcp-1, 0) // 减去表头

	// UDP连接数
	udpOut, _ := exec.Command("sh", "-c", "ss -u | wc -l").Output()
	udp, _ = strconv.Atoi(strings.TrimSpace(string(udpOut)))
	udp = max(udp-1, 0)

	// 进程数
	procOut, _ := exec.Command("sh", "-c", "ps -ef | wc -l").Output()
	process, _ = strconv.Atoi(strings.TrimSpace(string(procOut)))
	process = max(process-2, 0)

	// 线程数
	threadOut, _ := exec.Command("sh", "-c", "ps -eLf | wc -l").Output()
	thread, _ = strconv.Atoi(strings.TrimSpace(string(threadOut)))
	thread = max(thread-2, 0)

	return tcp, udp, process, thread
}

func getCustomMonitorData() string {
	monitorServer.RLock()
	defer monitorServer.RUnlock()

	var parts []string
	for name, ms := range monitorServer.servers {
		part := fmt.Sprintf("%s\\t解析: %d\\t连接: %d\\t下载: %d\\t在线率: <code>%.1f%%</code>",
			name, ms.DnsTime, ms.ConnectTime, ms.DownloadTime, ms.OnlineRate*100)
		parts = append(parts, part)
	}
	return strings.Join(parts, "<br>")
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}