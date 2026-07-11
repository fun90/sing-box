package connlimit

import (
	"sync"
	"testing"
)

// New 在两个上限都 <=0 时应返回 nil（不限制、无开销）。
func TestNewNilWhenUnlimited(t *testing.T) {
	if New(0, 0) != nil {
		t.Fatal("New(0,0) 应返回 nil")
	}
	if New(-1, -1) != nil {
		t.Fatal("New(-1,-1) 应返回 nil")
	}
	if New(1, 0) == nil {
		t.Fatal("New(1,0) 不应返回 nil")
	}
	if New(0, 1) == nil {
		t.Fatal("New(0,1) 不应返回 nil")
	}
}

// 同一 IP 的多条连接共享一个 IP 名额（引用计数），不受 maxIPs 限制。
func TestSameIPMultipleConnections(t *testing.T) {
	l := New(1, 0) // 最多 1 个不同 IP，连接数不限
	if !l.Acquire("1.1.1.1") {
		t.Fatal("首条连接应放行")
	}
	if !l.Acquire("1.1.1.1") {
		t.Fatal("同 IP 第二条连接应放行（共享名额）")
	}
	if !l.Acquire("1.1.1.1") {
		t.Fatal("同 IP 第三条连接应放行")
	}
	// 新 IP 应被拒绝（已达 1 个 IP 上限）
	if l.Acquire("2.2.2.2") {
		t.Fatal("新 IP 超过 maxIPs 应被拒绝")
	}
}

// maxIPs 达到上限后，新 IP 被拒；释放全部旧 IP 连接后新 IP 可接入。
func TestMaxIPsLimitAndRelease(t *testing.T) {
	l := New(2, 0)
	if !l.Acquire("1.1.1.1") || !l.Acquire("2.2.2.2") {
		t.Fatal("前两个不同 IP 应放行")
	}
	if l.Acquire("3.3.3.3") {
		t.Fatal("第三个 IP 应被拒绝")
	}
	// 释放 1.1.1.1，其名额腾出
	l.Release("1.1.1.1")
	if !l.Acquire("3.3.3.3") {
		t.Fatal("释放后新 IP 应可接入")
	}
}

// maxConnections 限制连接总数，与 IP 无关。
func TestMaxConnectionsLimit(t *testing.T) {
	l := New(0, 2) // 连接总数最多 2，IP 不限
	if !l.Acquire("1.1.1.1") || !l.Acquire("2.2.2.2") {
		t.Fatal("前两条连接应放行")
	}
	if l.Acquire("3.3.3.3") {
		t.Fatal("第三条连接应被拒绝（连接总数上限）")
	}
	l.Release("1.1.1.1")
	if !l.Acquire("3.3.3.3") {
		t.Fatal("释放一条后应可再接入")
	}
}

// 连接数上限优先于 IP 上限判断：同 IP 多连接也受连接总数约束。
func TestMaxConnectionsWithSameIP(t *testing.T) {
	l := New(5, 2) // IP 上限 5，连接上限 2
	if !l.Acquire("1.1.1.1") || !l.Acquire("1.1.1.1") {
		t.Fatal("同 IP 前两条应放行")
	}
	if l.Acquire("1.1.1.1") {
		t.Fatal("同 IP 第三条应被连接总数上限拒绝")
	}
}

// 对未 Acquire 成功的 IP 调用 Release 是安全的空操作。
func TestReleaseUnknownIP(t *testing.T) {
	l := New(1, 0)
	l.Release("9.9.9.9") // 不应 panic，不应影响计数
	if !l.Acquire("1.1.1.1") {
		t.Fatal("Release 未知 IP 后正常 Acquire 应放行")
	}
}

// 拒绝时不产生副作用：被拒的 Acquire 不占用名额。
func TestRejectNoSideEffect(t *testing.T) {
	l := New(1, 0)
	l.Acquire("1.1.1.1")
	// 连续多次被拒
	for i := 0; i < 5; i++ {
		if l.Acquire("2.2.2.2") {
			t.Fatal("应持续被拒")
		}
	}
	// 释放原 IP 后，新 IP 恰好可接入一个（证明之前的拒绝没有泄漏占用）
	l.Release("1.1.1.1")
	if !l.Acquire("2.2.2.2") {
		t.Fatal("释放后新 IP 应可接入")
	}
	if l.Acquire("3.3.3.3") {
		t.Fatal("再来一个新 IP 应被拒（名额只有 1）")
	}
}

// 并发 Acquire/Release 不应 panic 或计数错乱（配合 -race 检测）。
func TestConcurrentAcquireRelease(t *testing.T) {
	l := New(100, 0)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ip := "10.0.0." + string(rune('0'+n%10))
			for j := 0; j < 100; j++ {
				if l.Acquire(ip) {
					l.Release(ip)
				}
			}
		}(i)
	}
	wg.Wait()
	// 全部释放后，应能重新占满 100 个新 IP
	for i := 0; i < 100; i++ {
		ip := "172.16." + string(rune('0'+i/10)) + "." + string(rune('0'+i%10))
		_ = l.Acquire(ip)
	}
}
