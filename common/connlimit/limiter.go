// Package connlimit 提供按用户维度的单节点连接数 / 去重 IP 数限制。
//
// 该限制是「单节点兜底」闸门：每个 sing-box 内核只统计本节点上单个用户的
// 活跃连接与不同客户端 IP，防止单点被打爆。跨节点总量限制由 accountquota
// 包（读取中心下发的配额表）负责，两者是相互独立的两道闸门。
//
// 设计参考 common/ratelimit：每个 inbound 在 NewInbound 时按用户下标构建
// []*Limiter，下标与用户下标对齐；零值上限返回 nil，表示不限制、无额外开销。
package connlimit

import "sync"

// Limiter 限制单个用户在本节点上同时活跃的连接数与不同客户端 IP 数。
//
// 同一 IP 的多条并发连接共享一个 IP 名额（引用计数），因此正常单设备用户
// 的多连接不会被 IP 维度误伤；连接数维度则统计该用户的全部活跃连接。
type Limiter struct {
	mu             sync.Mutex
	maxIPs         int
	maxConnections int
	// key: 客户端 IP 字符串；value: 该 IP 当前活跃连接数（引用计数）
	ipRefs     map[string]int
	totalConns int
}

// New 返回一个限制器。maxIPs <= 0 且 maxConnections <= 0 时返回 nil，
// 表示该用户不受单节点兜底限制、无额外开销。
func New(maxIPs, maxConnections int) *Limiter {
	if maxIPs <= 0 && maxConnections <= 0 {
		return nil
	}
	return &Limiter{
		maxIPs:         maxIPs,
		maxConnections: maxConnections,
		ipRefs:         make(map[string]int),
	}
}

// Acquire 尝试为来自 ip 的新连接占用名额：
//   - maxConnections > 0 且当前连接总数已达上限 → 拒绝，返回 false。
//   - 为新 IP 且 maxIPs > 0 且当前不同 IP 数已达上限 → 拒绝，返回 false。
//   - 否则占用（同 IP 引用计数 +1，连接总数 +1），返回 true。
//
// 拒绝时不产生任何副作用，调用方无需 Release。
func (l *Limiter) Acquire(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.maxConnections > 0 && l.totalConns >= l.maxConnections {
		return false
	}

	_, existing := l.ipRefs[ip]
	if !existing && l.maxIPs > 0 && len(l.ipRefs) >= l.maxIPs {
		return false
	}

	l.ipRefs[ip]++
	l.totalConns++
	return true
}

// Release 在连接关闭时归还名额：引用计数 -1，归零则从集合移除；连接总数 -1。
// 对未经 Acquire 成功的 ip 调用是安全的空操作。
func (l *Limiter) Release(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	n, ok := l.ipRefs[ip]
	if !ok {
		return
	}
	if n <= 1 {
		delete(l.ipRefs, ip)
	} else {
		l.ipRefs[ip] = n - 1
	}
	if l.totalConns > 0 {
		l.totalConns--
	}
}
