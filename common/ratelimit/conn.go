package ratelimit

import (
	"context"
	"net"

	"golang.org/x/time/rate"
)

type rateLimitedConn struct {
	net.Conn
	ctx      context.Context
	upload   *rate.Limiter // client→server: throttle Read
	download *rate.Limiter // server→client: throttle Write
}

// WrapConn wraps conn with rate limiting. upload limits reading from conn (client→server),
// download limits writing to conn (server→client). Either may be nil for no limit.
func WrapConn(ctx context.Context, conn net.Conn, upload, download *rate.Limiter) net.Conn {
	return &rateLimitedConn{
		Conn:     conn,
		ctx:      ctx,
		upload:   upload,
		download: download,
	}
}

func (c *rateLimitedConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if n > 0 && c.upload != nil {
		waitN(c.ctx, c.upload, n)
	}
	return
}

func (c *rateLimitedConn) Write(b []byte) (n int, err error) {
	if c.download == nil {
		return c.Conn.Write(b)
	}
	total := 0
	for total < len(b) {
		chunk := c.download.Burst()
		if remaining := len(b) - total; remaining < chunk {
			chunk = remaining
		}
		if err2 := c.download.WaitN(c.ctx, chunk); err2 != nil {
			return total, err2
		}
		written, err2 := c.Conn.Write(b[total : total+chunk])
		total += written
		if err2 != nil {
			return total, err2
		}
	}
	return total, nil
}

// waitN waits for n tokens, chunking if n > burst to avoid panic.
func waitN(ctx context.Context, l *rate.Limiter, n int) {
	burst := l.Burst()
	for n > 0 {
		chunk := n
		if chunk > burst {
			chunk = burst
		}
		_ = l.WaitN(ctx, chunk)
		n -= chunk
	}
}
