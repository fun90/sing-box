package ratelimit

import (
	"golang.org/x/time/rate"
)

type Limiters struct {
	Download *rate.Limiter
	Upload   *rate.Limiter
}

// NewLimiters creates a Limiters pair from Mbps values (SI: 1 Mbps = 125000 bytes/s).
// A zero value means unlimited for that direction.
func NewLimiters(downloadMbps, uploadMbps float64) *Limiters {
	l := &Limiters{}
	if downloadMbps > 0 {
		l.Download = newLimiter(downloadMbps)
	}
	if uploadMbps > 0 {
		l.Upload = newLimiter(uploadMbps)
	}
	if l.Download == nil && l.Upload == nil {
		return nil
	}
	return l
}

func newLimiter(mbps float64) *rate.Limiter {
	bytesPerSec := mbps * 1_000_000 / 8
	burst := int(bytesPerSec)
	if burst < 64*1024 {
		burst = 64 * 1024
	}
	return rate.NewLimiter(rate.Limit(bytesPerSec), burst)
}
