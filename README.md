# ServerStatus-Go

适配 [cppla/ServerStatus](https://github.com/cppla/ServerStatus) 服务端的 Golang 客户端，负责采集本机运行状态并通过 TCP 协议上报。

[![Build](https://github.com/LM-Home/ServerStatus-Go/actions/workflows/main.yml/badge.svg)](https://github.com/LM-Home/ServerStatus-Go/actions)
[![Go Version](https://img.shields.io/badge/Go-1.26.1-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![Protocol](https://img.shields.io/badge/Protocol-cppla%2FServerStatus-4EC5D4.svg)](https://github.com/cppla/ServerStatus)
[![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20FreeBSD-blue.svg)]()

## 功能特性

| 类别 | 说明 |
|------|------|
| 系统监控 | CPU / 内存 / 磁盘 / Swap / 负载 / 运行时长 |
| 网络监控 | 实时网速、累计流量、磁盘 IO 读写 |
| 连通性检测 | IPv4 / IPv6 双栈在线检测 |
| 延迟探测 | 联通 / 电信 / 移动三大运营商延迟与丢包率 |
| 自定义监控 | 支持服务端下发的 HTTP / HTTPS / TCP 探测任务 |
| 连接统计 | TCP / UDP 连接数、进程数、线程数 |
| 连接保活 | TCP Keepalive，防止 Docker 等环境下 conntrack 清理空闲连接 |
| 优雅退出 | 收到 SIGINT / SIGTERM 后安全关闭 |

## 快速开始

### 编译

```bash
go build -o serverStatus .
```

### 运行

**命令行参数方式：**

```bash
./serverStatus -host 1.2.3.4 -port 35601 -user myuser -password mypass -interval 1.5
```

**DSN 方式：**

```bash
./serverStatus -dsn "myuser:mypass@1.2.3.4:35601" -interval 1.5
```

## 命令行参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `-host` | string | (必填) | 服务端地址 |
| `-port` | int | `35601` | 服务端端口 |
| `-user` | string | (必填) | 客户端用户名 |
| `-password` | string | (必填) | 客户端密码 |
| `-dsn` | string | - | DSN 格式: `username:password@host:port`，与上述参数二选一 |
| `-interval` | float | `1.0` | 数据上报间隔（秒） |
| `-vnstat` | bool | `false` | 使用 vnstat 获取网络流量（仅 Linux） |
| `-cu` | string | `cu.tz.cloudcpp.com` | 联通探针地址 |
| `-ct` | string | `ct.tz.cloudcpp.com` | 电信探针地址 |
| `-cm` | string | `cm.tz.cloudcpp.com` | 移动探针地址 |
| `-proto` | string | `ipv4` | 探针协议偏好（`ipv4` 或 `ipv6`） |
| `-probePort` | int | `80` | 探针探测端口 |
| `-debug` | bool | `false` | 开启调试日志（输出更详细的运行信息） |

## 部署示例

以 systemd 服务方式常驻运行（保存为 `/etc/systemd/system/serverstatus-go.service`）：

```ini
[Unit]
Description=ServerStatus-Go Client
After=network.target

[Service]
ExecStart=/usr/local/bin/serverStatus -host 1.2.3.4 -port 35601 -user myuser -password mypass
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now serverstatus-go
```

## 交叉编译

项目支持静态编译，可跨平台构建，方便直接部署到目标机器：

```bash
GOOS=linux   GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-s -w" -tags netgo -o serverStatus-linux-amd64
GOOS=linux   GOARCH=arm64 CGO_ENABLED=0 go build -ldflags "-s -w" -tags netgo -o serverStatus-linux-arm64
GOOS=freebsd GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-s -w" -tags netgo -o serverStatus-freebsd-amd64
```

## 运行原理

1. 解析命令行参数并校验配置（`-dsn` 会自动拆分为 user/password/host/port）
2. 初始化日志（默认 Info 级别，`-debug` 时输出 Debug 级别）
3. 初始化数据存储及采集器、监控器、发送器三个模块
4. 注册 SIGINT / SIGTERM 信号处理
5. 采集器与监控器在后台 goroutine 中按 `-interval` 周期性采集数据
6. 发送器连接服务端并完成认证，随后按间隔上报状态数据
7. 收到终止信号后取消所有任务并优雅退出

## 项目结构

```
ServerStatus-Go/
├── main.go               # 入口：组装各模块、启动后台任务
├── .github/workflows/    # GitHub Actions 构建发布
└── pkg/
    ├── collector/        # 系统数据采集（CPU/内存/磁盘/负载/网络/连接数）
    ├── common/           # 数据类型定义与并发安全存储
    ├── config/           # 命令行参数解析与校验
    ├── monitor/          # 网络探测（运营商延迟、双栈检测、自定义监控）
    └── sender/           # TCP 连接、认证、状态上报
```

## 依赖

- [json-iterator/go](https://github.com/json-iterator/go) — 高性能 JSON 序列化
- [shirou/gopsutil](https://github.com/shirou/gopsutil) — 跨平台系统信息采集

## CI/CD

GitHub Actions 在手动触发（`workflow_dispatch`）或仓库事件（`repository_dispatch`）时，自动构建多架构二进制并发布 Release：

| 目标平台 | 架构 |
|----------|------|
| Linux | 386 / amd64 / arm64 |
| FreeBSD | amd64 |
