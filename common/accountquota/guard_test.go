package accountquota

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func writeTable(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("写入测试文件失败: %v", err)
	}
	return p
}

// 文件缺失时 fail-open：不阻断任何账户。
func TestBlockedFailOpenMissingFile(t *testing.T) {
	g := NewGuard(filepath.Join(t.TempDir(), "nonexistent.json"), nil)
	if blocked, _ := g.Blocked("A10001"); blocked {
		t.Fatal("文件缺失应 fail-open，不阻断")
	}
}

// 正常表内、未过期、账户在黑名单 → 阻断，且 reason 含维度与数值。
func TestBlockedHit(t *testing.T) {
	now := time.Now().Unix()
	content := `{
		"schemaVersion": 1,
		"generatedAtEpochSeconds": ` + strconv.FormatInt(now, 10) + `,
		"ttlSeconds": 15,
		"blockedAccounts": {
			"A10001": { "reason": "connections", "total": 12, "limit": 10 }
		}
	}`
	p := writeTable(t, t.TempDir(), "quota.json", content)
	g := NewGuard(p, nil)

	blocked, reason := g.Blocked("A10001")
	if !blocked {
		t.Fatal("黑名单账户应被阻断")
	}
	if reason != "connections 12/10" {
		t.Fatalf("reason 不符，期望 \"connections 12/10\"，实际 %q", reason)
	}
	// 不在黑名单的账户放行
	if b, _ := g.Blocked("A99999"); b {
		t.Fatal("非黑名单账户应放行")
	}
}

// 表已过期（now-generatedAt > ttl）时 fail-open。
func TestBlockedFailOpenExpired(t *testing.T) {
	old := time.Now().Unix() - 100
	content := `{
		"schemaVersion": 1,
		"generatedAtEpochSeconds": ` + strconv.FormatInt(old, 10) + `,
		"ttlSeconds": 15,
		"blockedAccounts": { "A10001": { "reason": "ips", "total": 4, "limit": 3 } }
	}`
	p := writeTable(t, t.TempDir(), "quota.json", content)
	g := NewGuard(p, nil)
	if blocked, _ := g.Blocked("A10001"); blocked {
		t.Fatal("表过期应 fail-open，不阻断")
	}
}

// schema 版本不符时 fail-open。
func TestBlockedFailOpenBadSchema(t *testing.T) {
	now := time.Now().Unix()
	content := `{
		"schemaVersion": 999,
		"generatedAtEpochSeconds": ` + strconv.FormatInt(now, 10) + `,
		"ttlSeconds": 15,
		"blockedAccounts": { "A10001": { "reason": "ips", "total": 4, "limit": 3 } }
	}`
	p := writeTable(t, t.TempDir(), "quota.json", content)
	g := NewGuard(p, nil)
	if blocked, _ := g.Blocked("A10001"); blocked {
		t.Fatal("schema 不符应 fail-open，不阻断")
	}
}

// 解析失败（坏 JSON）时 fail-open。
func TestBlockedFailOpenBadJSON(t *testing.T) {
	p := writeTable(t, t.TempDir(), "quota.json", "{not valid json")
	g := NewGuard(p, nil)
	if blocked, _ := g.Blocked("A10001"); blocked {
		t.Fatal("坏 JSON 应 fail-open，不阻断")
	}
}

// 空账户号直接放行。
func TestBlockedEmptyAccount(t *testing.T) {
	g := NewGuard(filepath.Join(t.TempDir(), "x.json"), nil)
	if blocked, _ := g.Blocked(""); blocked {
		t.Fatal("空账户号应放行")
	}
}

// reload 后能感知文件内容变化。
func TestReloadPicksUpChanges(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().Unix()
	p := filepath.Join(dir, "quota.json")

	// 初始：无黑名单
	writeTable(t, dir, "quota.json", `{"schemaVersion":1,"generatedAtEpochSeconds":`+
		strconv.FormatInt(now, 10)+`,"ttlSeconds":15,"blockedAccounts":{}}`)
	g := NewGuard(p, nil)
	if b, _ := g.Blocked("A10001"); b {
		t.Fatal("初始不应阻断")
	}

	// 更新文件：加入黑名单
	writeTable(t, dir, "quota.json", `{"schemaVersion":1,"generatedAtEpochSeconds":`+
		strconv.FormatInt(time.Now().Unix(), 10)+`,"ttlSeconds":15,`+
		`"blockedAccounts":{"A10001":{"reason":"ips","total":5,"limit":2}}}`)
	g.reload()
	if b, _ := g.Blocked("A10001"); !b {
		t.Fatal("reload 后应感知新黑名单")
	}
}

// ResolvePath 优先环境变量。
func TestResolvePath(t *testing.T) {
	t.Setenv(EnvPath, "/custom/path.json")
	if got := ResolvePath(); got != "/custom/path.json" {
		t.Fatalf("应返回环境变量路径，实际 %q", got)
	}
	os.Unsetenv(EnvPath)
	if got := ResolvePath(); got != DefaultPath {
		t.Fatalf("应返回默认路径，实际 %q", got)
	}
}
