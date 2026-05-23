package ratelimit

import (
	"context"

	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"golang.org/x/time/rate"
)

type rateLimitedPacketConn struct {
	N.PacketConn
	ctx      context.Context
	upload   *rate.Limiter // client→server: throttle after ReadPacket
	download *rate.Limiter // server→client: throttle before WritePacket
}

// WrapPacketConn wraps conn with rate limiting. upload limits reading (client→server),
// download limits writing (server→client). Either may be nil for no limit.
func WrapPacketConn(ctx context.Context, conn N.PacketConn, upload, download *rate.Limiter) N.PacketConn {
	return &rateLimitedPacketConn{
		PacketConn: conn,
		ctx:        ctx,
		upload:     upload,
		download:   download,
	}
}

func (c *rateLimitedPacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	destination, err := c.PacketConn.ReadPacket(buffer)
	if err == nil && c.upload != nil && buffer.Len() > 0 {
		waitN(c.ctx, c.upload, buffer.Len())
	}
	return destination, err
}

func (c *rateLimitedPacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	if c.download != nil && buffer.Len() > 0 {
		waitN(c.ctx, c.download, buffer.Len())
	}
	return c.PacketConn.WritePacket(buffer, destination)
}
