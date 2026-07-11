package accountquota

import (
	"context"
	"sync"

	"github.com/sagernet/sing/common/logger"
)

// 全局单例：配额表是节点级共享资源（所有 inbound 读同一个文件），
// 因此用一个进程级 Guard，避免每个 inbound 各自加载、各起一个重载 goroutine。

var (
	globalOnce  sync.Once
	globalGuard *Guard
)

// Global 返回进程级共享的 Guard，首次调用时创建并启动后台重载。
// logger 只在首次创建时生效；ctx 用于控制后台 goroutine 生命周期。
func Global(ctx context.Context, logger logger.Logger) *Guard {
	globalOnce.Do(func() {
		globalGuard = NewGuard("", logger)
		globalGuard.Start(ctx)
	})
	return globalGuard
}
