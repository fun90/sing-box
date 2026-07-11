// Package accountquota 读取 AirOpsCat 中心下发的「跨节点总量」配额表，
// 供各协议 inbound 在认证成功后判断某账户是否已被判定超总限。
//
// 配额表由节点侧 agent 通过一次 guard-sync 请求从中心取回并原子写入本地文件
// （默认 /run/airopscat/account-quota.json，tmpfs）。sing-box 内核只读该文件，
// 不发起任何网络请求。文件格式：
//
//	{
//	  "schemaVersion": 1,
//	  "generatedAtEpochSeconds": 1778640000,
//	  "ttlSeconds": 15,
//	  "blockedAccounts": {
//	    "A10001": { "reason": "connections", "total": 12, "limit": 10 }
//	  }
//	}
//
// 只列出「已判定超总限」的账户（黑名单语义），表通常很小。
//
// fail-open 原则：文件缺失、解析失败、schema 不符或表已过期（now-generatedAt
// > ttlSeconds）时，一律视为「不阻断」，避免中心故障或 agent 中断导致节点全量
// 拒绝连接（可用性优先）。
package accountquota

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing/common/logger"
)

// SchemaVersion 是本内核支持的配额表结构版本，须与 agent 写入的一致。
const SchemaVersion = 1

// DefaultPath 是配额表默认路径；可用环境变量 EnvPath 覆盖。
const (
	DefaultPath = "/run/airopscat/account-quota.json"
	EnvPath     = "AIROPSCAT_ACCOUNT_QUOTA_FILE"
	// reloadInterval 是后台重载周期。远小于典型 TTL(15s) 与同步周期(5s)，
	// 保证新配额及时生效；重载只读一个小文件，开销可忽略。
	reloadInterval = 2 * time.Second
)

// BlockedEntry 是被阻断账户的明细，供日志与诊断使用。
type BlockedEntry struct {
	Reason string `json:"reason"` // "connections" 或 "ips"
	Total  int    `json:"total"`
	Limit  int    `json:"limit"`
}

type table struct {
	SchemaVersion           int                     `json:"schemaVersion"`
	GeneratedAtEpochSeconds int64                   `json:"generatedAtEpochSeconds"`
	TTLSeconds              int                     `json:"ttlSeconds"`
	BlockedAccounts         map[string]BlockedEntry `json:"blockedAccounts"`
}

// Guard 持有配额表的最新快照，并后台周期性重载。查询为无锁 O(1)。
type Guard struct {
	path    string
	logger  logger.Logger
	current atomic.Pointer[table]
}

// NewGuard 创建 Guard 并立即尝试加载一次（失败不报错，保持 fail-open）。
// path 为空时使用 ResolvePath 的结果。
func NewGuard(path string, logger logger.Logger) *Guard {
	if path == "" {
		path = ResolvePath()
	}
	g := &Guard{path: path, logger: logger}
	g.reload()
	return g
}

// ResolvePath 返回配额表路径：优先环境变量，否则默认路径。
func ResolvePath() string {
	if v := os.Getenv(EnvPath); v != "" {
		return v
	}
	return DefaultPath
}

// Start 启动后台重载 goroutine，直到 ctx 取消。可安全多次调用前只调用一次。
func (g *Guard) Start(ctx context.Context) {
	go g.loop(ctx)
}

func (g *Guard) loop(ctx context.Context) {
	ticker := time.NewTicker(reloadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.reload()
		}
	}
}

func (g *Guard) reload() {
	data, err := os.ReadFile(g.path)
	if err != nil {
		// 文件缺失是常态（未启用防共享 / agent 尚未写入），清空快照并 fail-open。
		g.current.Store(nil)
		return
	}
	var t table
	if err := json.Unmarshal(data, &t); err != nil {
		if g.logger != nil {
			g.logger.Warn("account-quota: parse ", g.path, " failed: ", err)
		}
		g.current.Store(nil)
		return
	}
	if t.SchemaVersion != SchemaVersion {
		if g.logger != nil {
			g.logger.Warn("account-quota: unsupported schemaVersion ", t.SchemaVersion, " in ", g.path)
		}
		g.current.Store(nil)
		return
	}
	g.current.Store(&t)
}

// Blocked 返回该账户是否已被中心判定超总限，以及可读的原因（含维度与数值，
// 供 inbound 日志诊断）。表缺失 / 过期 / 账户不在黑名单时返回 false（fail-open）。
//
// 返回签名 (bool, string) 与 connlimit.Guard 接口对齐，使 Guard 可直接作为
// 两道闸门检查的跨节点配额实现传入，无需适配层。
func (g *Guard) Blocked(accountNo string) (bool, string) {
	if accountNo == "" {
		return false, ""
	}
	t := g.current.Load()
	if t == nil {
		return false, ""
	}
	// 过期即 fail-open：ttlSeconds <= 0 视为无效表。
	if t.TTLSeconds <= 0 {
		return false, ""
	}
	if time.Now().Unix()-t.GeneratedAtEpochSeconds > int64(t.TTLSeconds) {
		return false, ""
	}
	entry, ok := t.BlockedAccounts[accountNo]
	if !ok {
		return false, ""
	}
	// reason 形如 "connections 12/10"，便于 inbound 日志定位超限维度与数值。
	return true, entry.Reason + " " + strconv.Itoa(entry.Total) + "/" + strconv.Itoa(entry.Limit)
}
