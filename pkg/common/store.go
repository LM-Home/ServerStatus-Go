package common

import (
	"sync"
)

type Store struct {
	mu sync.RWMutex

	Uptime      uint64
	Load1       float64
	Load5       float64
	Load15      float64
	MemoryTotal uint64
	MemoryUsed  uint64
	SwapTotal   uint64
	SwapUsed    uint64
	HddTotal    uint64
	HddUsed     uint64
	CPU         float64

	// Network
	NetworkRx int64
	NetworkTx int64
	NetworkIn uint64
	NetworkOut uint64
	
	// Speed helpers
	AvgRx int64
	AvgTx int64
	NetClock float64
	NetDiff float64

	// Online
	Online4 bool
	Online6 bool

	// Ping
	PingCU float64
	PingCM float64
	PingCT float64
	TimeCU int
	TimeCT int
	TimeCM int

	// Stats
	TCP     int
	UDP     int
	Process int
	Thread  int

	// IO
	IoRead  int64
	IoWrite int64
	
	// IO helpers
	LastIORead int64
	LastIOWrite int64

	// Custom
	Custom string

	// Monitors
	MonitorServers map[string]*MonitorServer
}

func NewStore() *Store {
	return &Store{
		MonitorServers: make(map[string]*MonitorServer),
	}
}

func (s *Store) Update(fn func(*Store)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s)
}

func (s *Store) RLock() {
	s.mu.RLock()
}

func (s *Store) RUnlock() {
	s.mu.RUnlock()
}
