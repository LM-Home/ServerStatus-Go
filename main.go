package main

import (
	"bufio"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/shirou/gopsutil/v3/disk"
)

var (
	Server                 = flag.String("host", "", "主机地址")
	Port                   = flag.Int("port", 35601, "主机端口")
	User                   = flag.String("user", "", "客户端用户名")
	Password               = flag.String("password", "", "客户端密码")
	Interval               = flag.Float64("interval", 1.0, "数据发送间隔(秒)")
	DSN                    = flag.String("dsn", "", "DSN 格式: username:password@host:port")
	IsVnstat               = flag.Bool("vnstat", false, "使用 vnstat 获取网络流量(仅Linux)")
	CU                     = flag.String("cu", "cu.tz.cloudcpp.com", "CU 探针地址")
	CT                     = flag.String("ct", "ct.tz.cloudcpp.com", "CT 探针地址")
	CM                     = flag.String("cm", "cm.tz.cloudcpp.com", "CM 探针地址")
	ProbePort              = flag.Int("probePort", 80, "探针端口")
	CachedFs               = make(map[string]struct{})
	ProbeProtocolPrefer    = flag.String("proto", "ipv4", "探针协议偏好(ipv4或ipv6)")
	ValidFs                = []string{"ext4", "ext3", "ext2", "reiserfs", "jfs", "btrfs", "fuseblk", "zfs", "simfs", "ntfs", "fat32", "exfat", "xfs", "apfs"}
	PingPacketHistoryLen   = 64
	OnlinePacketHistoryLen = 64
	timeCU, timeCT, timeCM int
	pingCU, pingCM, pingCT float64
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// 全局状态存储（带并发保护）
var (
	lostRate = sync.Map{} // key: mark(string), value: float64(%)
	pingTime = sync.Map{} // key: mark(string), value: int(ms)
	netSpeed = struct {
		sync.Mutex
		netrx int64
		nettx int64
		clock float64
		diff  float64
		avgrx int64
		avgtx int64
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
	Uptime      uint64          `json:"uptime"`
	Load1       jsoniter.Number `json:"load_1"`
	Load5       jsoniter.Number `json:"load_5"`
	Load15      jsoniter.Number `json:"load_15"`
	MemoryTotal uint64          `json:"memory_total"`
	MemoryUsed  uint64          `json:"memory_used"`
	SwapTotal   uint64          `json:"swap_total"`
	SwapUsed    uint64          `json:"swap_used"`
	HddTotal    uint64          `json:"hdd_total"`
	HddUsed     uint64          `json:"hdd_used"`
	CPU         jsoniter.Number `json:"cpu"`
	NetworkRx   int64           `json:"network_rx"`
	NetworkTx   int64           `json:"network_tx"`
	NetworkIn   uint64          `json:"network_in"`
	NetworkOut  uint64          `json:"network_out"`
	Online4     bool            `json:"online4,omitempty"`
	Online6     bool            `json:"online6,omitempty"`
	PingCU      float64         `json:"ping_10010"`
	PingCM      float64         `json:"ping_10086"`
	PingCT      float64         `json:"ping_189"`
	TimeCU      int             `json:"time_10010"`
	TimeCT      int             `json:"time_189"`
	TimeCM      int             `json:"time_10086"`
	TCP         int             `json:"tcp"`
	UDP         int             `json:"udp"`
	Process     int             `json:"process"`
	Thread      int             `json:"thread"`
	IoRead      int64           `json:"io_read"`
	IoWrite     int64           `json:"io_write"`
	Custom      string          `json:"custom"`
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
			log.Fatal("DSN 格式错误, 缺少 @ 符号, 应为 username:password@host:port")
		}
		auth := strings.Split(parts[0], ":")
		if len(auth) != 2 {
			log.Fatal("DSN 格式错误, 缺少 : 号符, 应为 username:password@host:port")
		}
		*User = auth[0]
		*Password = auth[1]

		addr := strings.Split(parts[1], ":")
		*Server = addr[0]
		if len(addr) == 2 {
			port, err := strconv.Atoi(addr[1])
			if err == nil {
				*Port = port
			}
		}
	}
}

// 验证参数有效性
func validateParams() {
	if *Port < 1 || *Port > 65535 {
		log.Fatal("端口号必须在1到65535之间")
	}
	if *Server == "" || *User == "" || *Password == "" {
		log.Fatal("主机地址、用户名和密码不能为空")
	}
	probeProtocolPrefer := strings.ToLower(*ProbeProtocolPrefer)
	switch probeProtocolPrefer {
	case "ipv4":
		*ProbeProtocolPrefer = "ip4"
	case "ipv6":
		*ProbeProtocolPrefer = "ip6"
	default:
		*ProbeProtocolPrefer = "ip"
	}
}

// 启动所有后台监控线程
func startBackgroundMonitors() {
	// 启动多目标Ping监测
	go pingWorker(*CU, "CU", *ProbePort)
	go pingWorker(*CT, "CT", *ProbePort)
	go pingWorker(*CM, "CM", *ProbePort)

	// 启动网络速率监测
	go netSpeedMonitor()

	// 启动磁盘IO监测
	go diskIOMonitor()
}

// pingWorker 多目标Ping监测工作线程
func pingWorker(host, mark string, port int) {
	lostCount := 0
	history := make([]int, 0, PingPacketHistoryLen)
	userInterval := time.Duration(*Interval) * time.Second
	interval := userInterval // 初始间隔

	for {
		// 解析IP（优先指定协议）
		ip, err := resolveIP(host)
		if err != nil {
			log.Printf("PingWorker %s: 解析IP失败: %v\n", mark, err)
			ip = host // 解析失败直接使用主机名
		}

		// 维护历史队列
		if len(history) >= PingPacketHistoryLen {
			if history[0] == 0 {
				lostCount--
			}
			interval = userInterval * 60 // 每次检查后增加间隔
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
		if len(history) > PingPacketHistoryLen/2 {
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

	prefer := strings.ToLower(*ProbeProtocolPrefer)
	ipAddr, err := net.ResolveIPAddr(prefer, host)
	if err != nil {
		return "", err
	}
	return ipAddr.IP.String(), nil
}

// netSpeedMonitor 网络速率监测
func netSpeedMonitor() {
	interval := time.Duration(*Interval) * time.Second
	netSpeed.avgrx = 0
	netSpeed.avgtx = 0
	netSpeed.clock = float64(time.Now().UnixNano()) / 1e9

	for {
		avgrx, avgtx, err := getNetBytes()
		if err != nil {
			log.Println("网络速率监测错误:", err)
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
	interval := time.Duration(*Interval) * time.Second

	for {
		// 第一次采样
		first, err := disk.IOCounters()
		if err != nil {
			log.Println("磁盘 IO 监测错误:", err)
			time.Sleep(interval)
			continue
		}

		time.Sleep(interval)

		// 第二次采样
		second, err := disk.IOCounters()
		if err != nil {
			log.Println("磁盘 IO 监测错误:", err)
			time.Sleep(interval)
			continue
		}

		// 计算差值
		var read, write int64
		for device, ioFir := range first {
			ioSec, ok := second[device]
			if !ok || ioFir.Name != ioSec.Name {
				continue
			}
			read += int64(ioSec.ReadBytes  - ioFir.ReadBytes )
			write += int64(ioSec.WriteBytes  - ioFir.WriteBytes)
		}

		diskIO.Lock()
		diskIO.read = read
		diskIO.write = write
		diskIO.Unlock()
	}
}


// 连接服务器并发送状态数据
func connect() {
	src := fmt.Sprintf("连接:%s:%d", *Server, *Port)
	log.Println(src)
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(*Server, strconv.Itoa(*Port)), 30*time.Second)
	if err != nil {
		log.Println("连接失败:", err)
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
		log.Println("处理监控配置错误:", err)
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
		log.Println("检测认证需求失败:", err)
		return false
	}

	// 发送认证信息
	_, err = conn.Write([]byte(*User + ":" + *Password + "\n"))
	if err != nil {
		log.Println("发送认证信息失败:", err)
		return false
	}

	// 验证认证结果
	n, err = conn.Read(buf)
	if err != nil || !strings.Contains(string(buf[:n]), "Authentication successful") {
		log.Println("认证失败:", string(buf[:n]), err)
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
		return 0, fmt.Errorf("未知的连接方式")
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
				Type: cfg.Type,
				host: cfg.Host,
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
	history := make([]int, 0, OnlinePacketHistoryLen)
	userInterval := time.Duration(ms.interval) * time.Second
	interval := userInterval // 初始间隔

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
		if len(history) >= OnlinePacketHistoryLen {
			if history[0] == 0 {
				lostCount--
			}
			history = history[1:]
			interval = userInterval * 5 // 每次检查后增加间隔
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

	// 创建一个 HTTP 客户端
	client := &http.Client{
		// 设置超时，包括建立连接、发送请求和接收响应的总时间
		Timeout: 5 * time.Second,
		// 使用自定义的 Transport，以复用 net.DialTimeout 的能力
		Transport: &http.Transport{
			// 配置 TLS，通过 InsecureSkipVerify: true 来跳过证书验证
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			DialContext: (&net.Dialer{
				Timeout: 5 * time.Second,
			}).DialContext,
		},
	}

	// 构造完整的 URL
	url := host

	// 发送 GET 请求
	resp, err := client.Get(url)
	if err != nil {
		log.Printf("请求失败: %v\n", err)
		return false, dnsTime, connectTime, 0
	}
	defer resp.Body.Close()

	// 从响应中获取状态码
	statusCode := resp.StatusCode
	code := strconv.Itoa(statusCode)
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
	interval := time.Duration(*Interval) * time.Second

	for {
		// 收集系统状态数据
		status := collectStatus(checkIP, &timer)

		// 序列化并发送
		data, err := json.Marshal(status)
		if err != nil {
			log.Println("序列化状态数据错误:", err)
			break
		}

		_, err = conn.Write([]byte("update " + string(data) + "\n"))
		if err != nil {
			log.Println("发送状态数据错误:", err)
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
	if *IsVnstat {
		netIn, netOut, err = trafficVnstat()
		if err != nil {
			log.Println("Vnstat 错误:", err)
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
	*timer -= *Interval

	// Ping数据
	if val, ok := lostRate.Load("CU"); ok {
		pingCU = val.(float64)
	}
	if val, ok := lostRate.Load("CM"); ok {
		pingCM = val.(float64)
	}
	if val, ok := lostRate.Load("CT"); ok {
		pingCT = val.(float64)
	}

	if val, ok := pingTime.Load("CU"); ok {
		timeCU = val.(int)
	}
	if val, ok := pingTime.Load("CM"); ok {
		timeCM = val.(int)
	}
	if val, ok := pingTime.Load("CT"); ok {
		timeCT = val.(int)
	}

	// 连接数和进程数
	tcp, udp, process, thread := getTupd()

	// 磁盘IO
	diskIO.Lock()
	ioRead, ioWrite := diskIO.read, diskIO.write
	diskIO.Unlock()

	// 自定义监控数据
	custom := getCustomMonitorData()

	return ServerStatus{
		Uptime:      getUptime(),
		Load1:       jsoniter.Number(fmt.Sprintf("%.2f", load1)),
		Load5:       jsoniter.Number(fmt.Sprintf("%.2f", load5)),
		Load15:      jsoniter.Number(fmt.Sprintf("%.2f", load15)),
		MemoryTotal: memTotal,
		MemoryUsed:  memUsed,
		SwapTotal:   swapTotal,
		SwapUsed:    swapTotal - swapFree,
		HddTotal:    hddTotal,
		HddUsed:     hddUsed,
		CPU:         jsoniter.Number(fmt.Sprintf("%.1f", cpu)),
		NetworkRx:   netRx,
		NetworkTx:   netTx,
		NetworkIn:   netIn,
		NetworkOut:  netOut,
		Online4:     online4,
		Online6:     online6,
		PingCU:      pingCU,
		PingCM:      pingCM,
		PingCT:      pingCT,
		TimeCU:      timeCU,
		TimeCT:      timeCT,
		TimeCM:      timeCM,
		TCP:         tcp,
		UDP:         udp,
		Process:     process,
		Thread:      thread,
		IoRead:      ioRead,
		IoWrite:     ioWrite,
		Custom:      custom,
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

	diskList, _ := disk.Partitions(false)
	devices := make(map[string]struct{})
	for _, disk := range diskList {
		_, ok := devices[disk.Device]
		if !ok && checkValidFs(disk.Fstype) {
			CachedFs[disk.Mountpoint] = struct{}{}
			devices[disk.Device] = struct{}{}
		}
	}

	for k := range CachedFs {
		usage, err := disk.Usage(k)
		if err != nil {
			delete(CachedFs, k)
			continue
		}
		total += usage.Total / 1024.0 / 1024.0
		used += usage.Used / 1024.0 / 1024.0
	}
	return total, used
}

func getCPU() float64 {
	// 读取初始CPU时间
	start, err := getCPUTime()
	if err != nil {
		return 0
	}
	time.Sleep(time.Duration(*Interval) * time.Second)

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
		return cpuTime{}, fmt.Errorf("CPU时间数据格式错误")
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

func checkValidFs(name string) bool {
	for _, v := range ValidFs {
		if strings.ToLower(name) == v {
			return true
		}
	}
	return false
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

func trafficVnstat() (uint64, uint64, error) {
	buf, err := exec.Command("vnstat", "--oneline", "b").Output()
	if err != nil {
		return 0, 0, err
	}
	vData := strings.Split(BytesToString(buf), ";")
	if len(vData) != 15 {
		// Not enough data available yet.
		return 0, 0, nil
	}
	netIn, err := strconv.ParseUint(vData[8], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	netOut, err := strconv.ParseUint(vData[9], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return netIn, netOut, nil
}

func getTupd() (tcp, udp, process, thread int) {
	// TCP连接数
	tcpOut, _ := exec.Command("sh", "-c", "ss -t | wc -l").Output()
	tcp, _ = strconv.Atoi(strings.TrimSpace(string(tcpOut)))
	tcp = max(tcp-1, 0) // 减去表头
	if tcp == 0 {
		tcpOut, _ = exec.Command("sh", "-c", "netstat -ant | grep '^tcp' | wc -l").Output()
		tcp, _ = strconv.Atoi(strings.TrimSpace(string(tcpOut)))
		tcp = max(tcp-1, 0) // 减去表头和空行
	}

	// UDP连接数
	udpOut, _ := exec.Command("sh", "-c", "ss -u | wc -l").Output()
	udp, _ = strconv.Atoi(strings.TrimSpace(string(udpOut)))
	udp = max(udp-1, 0)
	if udp == 0 {
		udpOut, _ = exec.Command("sh", "-c", "netstat -anu | grep '^udp' | wc -l").Output()
		udp, _ = strconv.Atoi(strings.TrimSpace(string(udpOut)))
		udp = max(udp-1, 0) // 减去表头和空行
	}

	// 进程数
	procOut, _ := exec.Command("sh", "-c", "ps -ef | wc -l").Output()
	process, _ = strconv.Atoi(strings.TrimSpace(string(procOut)))
	process = max(process-2, 0)

	// 线程数
	threadOut, _ := exec.Command("sh", "-c", "ps -eLf | wc -l").Output()
	thread, _ = strconv.Atoi(strings.TrimSpace(string(threadOut)))
	thread = max(thread-2, 0)
	if thread == 0 {
		threadOut, _ = exec.Command("sh", "-c", "grep -c ^Threads: /proc/*/status | wc -l").Output()
		thread, _ = strconv.Atoi(strings.TrimSpace(string(threadOut)))
		thread = max(thread-1, 0) // 减去表头
	}

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

func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
