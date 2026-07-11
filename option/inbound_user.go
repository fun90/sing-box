package option

type InboundUserSpeedOptions struct {
	DownloadMbps float64 `json:"download_mbps,omitempty"`
	UploadMbps   float64 `json:"upload_mbps,omitempty"`
	// MaxIPs 限制单个用户在本节点同时活跃的不同客户端 IP 数（单节点兜底）。
	// 同一 IP 的多条并发连接共享一个名额；<=0 表示不限制、无开销。
	MaxIPs int `json:"max_ips,omitempty"`
	// MaxConnections 限制单个用户在本节点的活跃连接总数（单节点兜底）。
	// <=0 表示不限制、无开销。
	MaxConnections int `json:"max_connections,omitempty"`
}
