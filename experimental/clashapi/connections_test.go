package clashapi

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/trafficcontrol"
	M "github.com/sagernet/sing/common/metadata"
)

func TestConnectionObjectMarshalJSONIncludesAuthUser(t *testing.T) {
	object := connectionObject(trafficcontrol.TrackerMetadata{
		Metadata: adapter.InboundContext{
			Inbound:     "node_34",
			InboundType: "vless",
			Network:     "udp",
			User:        "A001",
			Source: M.Socksaddr{
				Addr: M.ParseAddr("112.97.202.215"),
				Port: 21391,
			},
			Destination: M.Socksaddr{
				Addr: M.ParseAddr("31.13.92.37"),
				Port: 443,
				Fqdn: "www.youtube.com",
			},
		},
		CreatedAt: time.Unix(0, 0),
		Upload:    &atomic.Int64{},
		Download:  &atomic.Int64{},
	})

	payload, err := object.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON() 返回错误: %v", err)
	}

	jsonText := string(payload)
	if !strings.Contains(jsonText, `"authUser":"A001"`) {
		t.Fatalf("序列化结果未包含 authUser，实际内容: %s", jsonText)
	}
}
