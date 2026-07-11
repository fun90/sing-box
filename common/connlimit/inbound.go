package connlimit

import (
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// Guard 是跨节点总量配额的查询接口，由 accountquota 包实现。
// 放在此处以避免各协议 inbound 直接依赖具体实现；nil 表示未启用。
type Guard interface {
	// Blocked 返回该账户是否已被中心判定超出跨节点总量限制，以及原因。
	Blocked(accountNo string) (blocked bool, reason string)
}

// CheckAndTrack 在连接进入路由前执行两道闸门检查，返回是否放行以及
// 包装后的 onClose（放行时用于在连接关闭时归还单节点名额）。
//
//   - guard 非空且账户被中心判定超总限 → 返回 allowed=false。
//   - limiter 非空且单节点连接数 / IP 数超限 → 返回 allowed=false。
//   - 放行时，若 limiter 非空则占用名额，并把 Release 追加到 onClose，
//     保证连接关闭时精确归还一次。
//
// 拒绝时不产生任何名额占用，调用方直接静默关闭连接即可。
func CheckAndTrack(
	limiter *Limiter,
	guard Guard,
	accountNo string,
	source M.Socksaddr,
	onClose N.CloseHandlerFunc,
) (allowed bool, wrapped N.CloseHandlerFunc, reason string) {
	if guard != nil {
		if blocked, r := guard.Blocked(accountNo); blocked {
			return false, onClose, "global " + r
		}
	}
	if limiter == nil {
		return true, onClose, ""
	}
	ip := source.AddrString()
	if !limiter.Acquire(ip) {
		return false, onClose, "node-local limit"
	}
	release := N.OnceClose(func(error) {
		limiter.Release(ip)
	})
	return true, N.AppendClose(onClose, release), ""
}
