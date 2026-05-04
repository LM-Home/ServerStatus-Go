package collector

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"ServiceStatus/pkg/common"
	"ServiceStatus/pkg/config"
	"github.com/shirou/gopsutil/v3/disk"
)

var (
	ValidFs = []string{"ext4", "ext3", "ext2", "reiserfs", "jfs", "btrfs", "fuseblk", "zfs", "simfs", "ntfs", "fat32", "exfat", "xfs", "apfs"}
	cachedFs = make(map[string]struct{})
)

type Collector struct {
	cfg   *config.Config
	store *common.Store
}

func NewCollector(cfg *config.Config, store *common.Store) *Collector {
	return &Collector{cfg: cfg, store: store}
}

func (c *Collector) Start() {
	go c.netSpeedMonitor()
	go c.diskIOMonitor()
}

func (c *Collector) CollectAll() {
	uptime := c.getUptime()
	load1, load5, load15 := c.getLoad()
	memTotal, memUsed, swapTotal, swapUsed := c.getMemory()
	hddTotal, hddUsed := c.getDisk()
	cpu := c.getCPU()
	tcp, udp, process, thread := c.getTupd()

	var netIn, netOut uint64
	if c.cfg.IsVnstat {
		netIn, netOut, _ = c.trafficVnstat()
	} else {
		rx, tx, _ := c.getNetBytes()
		netIn, netOut = uint64(rx), uint64(tx)
	}

	c.store.Update(func(s *common.Store) {
		s.Uptime = uptime
		s.Load1 = load1
		s.Load5 = load5
		s.Load15 = load15
		s.MemoryTotal = memTotal
		s.MemoryUsed = memUsed
		s.SwapTotal = swapTotal
		s.SwapUsed = swapUsed
		s.HddTotal = hddTotal
		s.HddUsed = hddUsed
		s.CPU = cpu
		s.TCP = tcp
		s.UDP = udp
		s.Process = process
		s.Thread = thread
		s.NetworkIn = netIn
		s.NetworkOut = netOut
	})
}

func (c *Collector) getUptime() uint64 {
	data, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	parts := strings.Split(string(data), ".")
	uptime, _ := strconv.ParseUint(parts[0], 10, 64)
	return uptime
}

func (c *Collector) getLoad() (l1, l5, l15 float64) {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, 0, 0
	}
	parts := strings.Fields(string(data))
	if len(parts) >= 3 {
		l1, _ = strconv.ParseFloat(parts[0], 64)
		l5, _ = strconv.ParseFloat(parts[1], 64)
		l15, _ = strconv.ParseFloat(parts[2], 64)
	}
	return
}

func (c *Collector) getMemory() (total, used, swapTotal, swapUsed uint64) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return
	}
	memInfo := make(map[string]uint64)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) >= 2 {
			val, _ := strconv.ParseUint(parts[1], 10, 64)
			memInfo[parts[0]] = val
		}
	}
	total = memInfo["MemTotal:"]
	if total == 0 {
		return
	}
	free := memInfo["MemFree:"]
	buffers := memInfo["Buffers:"]
	cached := memInfo["Cached:"]
	sreclaimable := memInfo["SReclaimable:"]

	// Defensive check to prevent underflow
	overhead := free + buffers + cached + sreclaimable
	if total > overhead {
		used = total - overhead
	} else {
		used = 0
	}

	swapTotal = memInfo["SwapTotal:"]
	swapUsed = 0
	if swapTotal > memInfo["SwapFree:"] {
		swapUsed = swapTotal - memInfo["SwapFree:"]
	}
	return
}

func (c *Collector) getDisk() (total, used uint64) {
	diskList, _ := disk.Partitions(false)
	devices := make(map[string]struct{})
	for _, d := range diskList {
		if _, ok := devices[d.Device]; !ok && c.checkValidFs(d.Fstype) {
			cachedFs[d.Mountpoint] = struct{}{}
			devices[d.Device] = struct{}{}
		}
	}
	for k := range cachedFs {
		usage, err := disk.Usage(k)
		if err != nil {
			delete(cachedFs, k)
			continue
		}
		total += usage.Total / 1024 / 1024
		used += usage.Used / 1024 / 1024
	}
	return
}

func (c *Collector) checkValidFs(name string) bool {
	for _, v := range ValidFs {
		if strings.ToLower(name) == v {
			return true
		}
	}
	return false
}

func (c *Collector) getCPU() float64 {
	start, err := c.getCPUTime()
	if err != nil {
		return 0
	}
	time.Sleep(time.Duration(c.cfg.Interval) * time.Second)
	end, err := c.getCPUTime()
	if err != nil {
		return 0
	}
	total := end.Total() - start.Total()
	idle := end.idle - start.idle
	if total == 0 {
		return 0
	}
	return 100 - (float64(idle) / float64(total) * 100)
}

type cpuTime struct {
	user, nice, system, idle uint64
}

func (ct cpuTime) Total() uint64 {
	return ct.user + ct.nice + ct.system + ct.idle
}

func (c *Collector) getCPUTime() (cpuTime, error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return cpuTime{}, err
	}
	parts := strings.Fields(string(data))
	if len(parts) < 5 {
		return cpuTime{}, fmt.Errorf("invalid stat format")
	}
	u, _ := strconv.ParseUint(parts[1], 10, 64)
	n, _ := strconv.ParseUint(parts[2], 10, 64)
	s, _ := strconv.ParseUint(parts[3], 10, 64)
	i, _ := strconv.ParseUint(parts[4], 10, 64)
	return cpuTime{u, n, s, i}, nil
}

func (c *Collector) netSpeedMonitor() {
	interval := time.Duration(c.cfg.Interval) * time.Second
	c.store.Update(func(s *common.Store) {
		s.NetClock = float64(time.Now().UnixNano()) / 1e9
	})
	for {
		rx, tx, err := c.getNetBytes()
		if err != nil {
			time.Sleep(interval)
			continue
		}
		now := float64(time.Now().UnixNano()) / 1e9
		c.store.Update(func(s *common.Store) {
			s.NetDiff = now - s.NetClock
			if s.NetDiff > 0 && s.AvgRx > 0 { // Only calculate speed after first successful read
				s.NetworkRx = int64(float64(rx-s.AvgRx) / s.NetDiff)
				s.NetworkTx = int64(float64(tx-s.AvgTx) / s.NetDiff)
			}
			s.NetClock = now
			s.AvgRx = rx
			s.AvgTx = tx
		})
		time.Sleep(interval)
	}
}

func (c *Collector) getNetBytes() (rx, tx int64, err error) {
	file, err := os.Open("/proc/net/dev")
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	scanner.Scan()
	virtRegex := regexp.MustCompile(`lo|tun|docker|veth|br-|vmbr|vnet|kube`)
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) < 10 {
			continue
		}
		dev := strings.TrimSuffix(parts[0], ":")
		if virtRegex.MatchString(dev) {
			continue
		}
		r, _ := strconv.ParseInt(parts[1], 10, 64)
		t, _ := strconv.ParseInt(parts[9], 10, 64)
		rx += r
		tx += t
	}
	return rx, tx, scanner.Err()
}

func (c *Collector) diskIOMonitor() {
	interval := time.Duration(c.cfg.Interval) * time.Second
	for {
		first, err := disk.IOCounters()
		if err != nil {
			time.Sleep(interval)
			continue
		}
		time.Sleep(interval)
		second, err := disk.IOCounters()
		if err != nil {
			continue
		}
		var r, w int64
		for dev, ioFir := range first {
			if ioSec, ok := second[dev]; ok && ioFir.Name == ioSec.Name {
				r += int64(ioSec.ReadBytes - ioFir.ReadBytes)
				w += int64(ioSec.WriteBytes - ioFir.WriteBytes)
			}
		}
		c.store.Update(func(s *common.Store) {
			s.IoRead = r
			s.IoWrite = w
		})
	}
}

func (c *Collector) trafficVnstat() (uint64, uint64, error) {
	buf, err := exec.Command("vnstat", "--oneline", "b").Output()
	if err != nil {
		return 0, 0, err
	}
	vData := strings.Split(*(*string)(unsafe.Pointer(&buf)), ";")
	if len(vData) != 15 {
		return 0, 0, nil
	}
	rx, _ := strconv.ParseUint(vData[8], 10, 64)
	tx, _ := strconv.ParseUint(vData[9], 10, 64)
	return rx, tx, nil
}

func (c *Collector) getTupd() (tcp, udp, process, thread int) {
	tcp = c.countLines("ss -t", "netstat -ant | grep '^tcp'")
	udp = c.countLines("ss -u", "netstat -anu | grep '^udp'")
	process = c.countLines("ps -ef", "") - 1 // Simple approximation
	thread = c.countLines("ps -eLf", "grep -c ^Threads: /proc/*/status")
	return
}

func (c *Collector) countLines(cmd, fallback string) int {
	out, err := exec.Command("sh", "-c", cmd+" | wc -l").Output()
	if err != nil || strings.TrimSpace(string(out)) == "1" {
		if fallback != "" {
			out, _ = exec.Command("sh", "-c", fallback+" | wc -l").Output()
		}
	}
	val, _ := strconv.Atoi(strings.TrimSpace(string(out)))
	if val > 0 {
		return val - 1
	}
	return 0
}
