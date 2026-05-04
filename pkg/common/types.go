package common

import (
	jsoniter "github.com/json-iterator/go"
)

// MonitorServer 自定义服务器监控数据
type MonitorServer struct {
	Type         string  `json:"type"`
	DnsTime      int     `json:"dns_time"`
	ConnectTime  int     `json:"connect_time"`
	DownloadTime int     `json:"download_time"`
	OnlineRate   float64 `json:"online_rate"`
	Host         string  `json:"-"`
	Interval     int     `json:"-"`
	Stop         chan struct{} `json:"-"`
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
