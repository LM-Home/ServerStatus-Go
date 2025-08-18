# ServerStatus-Go

适配 https://github.com/cppla/ServerStatus 的 golang 客户端。

基于 https://github.com/cokemine/ServerStatus-goclient 修改而来。

## Usage

```
  -dsn string
        Input DSN, format: username:password@host:port
  -host string
        Input the host of the server
  -interval float
        Input the INTERVAL (default 2.0)
  -password string
        Input the client's password
  -port int
        Input the port of the server (default 35601)
  -user string
        Input the client's username
  -vnstat
        Use vnstat for traffic statistics, linux only
  -CU
        Set probe host of CU
  -CT
        Set probe host of CT
  -CM
        Set probe host of CM
  -proto
        Prefer proto of probe
  -probePort
        Proto port
```
