package hysteria

import (
	"context"
	"net"
	"os"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/common/accountquota"
	"github.com/sagernet/sing-box/common/connlimit"
	"github.com/sagernet/sing-box/common/listener"
	"github.com/sagernet/sing-box/common/ratelimit"
	"github.com/sagernet/sing-box/common/tls"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-quic/hysteria"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/auth"
	F "github.com/sagernet/sing/common/format"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func RegisterInbound(registry *inbound.Registry) {
	inbound.Register[option.HysteriaInboundOptions](registry, C.TypeHysteria, NewInbound)
}

type Inbound struct {
	inbound.Adapter
	router       adapter.Router
	logger       log.ContextLogger
	listener     *listener.Listener
	tlsConfig    tls.ServerConfig
	service      *hysteria.Service[int]
	userNameList []string
	userLimiters []*ratelimit.Limiters
	connLimiters []*connlimit.Limiter
	accountGuard *accountquota.Guard
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.HysteriaInboundOptions) (adapter.Inbound, error) {
	options.UDPFragmentDefault = true
	if options.TLS == nil || !options.TLS.Enabled {
		return nil, C.ErrTLSRequired
	}
	tlsConfig, err := tls.NewServer(ctx, logger, common.PtrValueOrDefault(options.TLS))
	if err != nil {
		return nil, err
	}
	inbound := &Inbound{
		Adapter: inbound.NewAdapter(C.TypeHysteria, tag),
		router:  router,
		logger:  logger,
		listener: listener.New(listener.Options{
			Context: ctx,
			Logger:  logger,
			Listen:  options.ListenOptions,
		}),
		tlsConfig: tlsConfig,
	}
	var sendBps, receiveBps uint64
	if options.Up.Value() > 0 {
		sendBps = options.Up.Value()
	} else {
		sendBps = uint64(options.UpMbps) * hysteria.MbpsToBps
	}
	if options.Down.Value() > 0 {
		receiveBps = options.Down.Value()
	} else {
		receiveBps = uint64(options.DownMbps) * hysteria.MbpsToBps
	}
	var udpTimeout time.Duration
	if options.UDPTimeout != 0 {
		udpTimeout = time.Duration(options.UDPTimeout)
	} else {
		udpTimeout = C.UDPTimeout
	}
	service, err := hysteria.NewService[int](hysteria.ServiceOptions{
		Context:       ctx,
		Logger:        logger,
		SendBPS:       sendBps,
		ReceiveBPS:    receiveBps,
		XPlusPassword: options.Obfs,
		TLSConfig:     tlsConfig,
		UDPTimeout:    udpTimeout,
		Handler:       inbound,

		// Legacy options

		ConnReceiveWindow:   options.ReceiveWindowConn,
		StreamReceiveWindow: options.ReceiveWindowClient,
		MaxIncomingStreams:  int64(options.MaxConnClient),
		DisableMTUDiscovery: options.DisableMTUDiscovery,
	})
	if err != nil {
		return nil, err
	}
	userList := make([]int, 0, len(options.Users))
	userNameList := make([]string, 0, len(options.Users))
	userPasswordList := make([]string, 0, len(options.Users))
	userLimiters := make([]*ratelimit.Limiters, len(options.Users))
	connLimiters := make([]*connlimit.Limiter, len(options.Users))
	for index, user := range options.Users {
		userList = append(userList, index)
		userNameList = append(userNameList, user.Name)
		var password string
		if user.AuthString != "" {
			password = user.AuthString
		} else {
			password = string(user.Auth)
		}
		userPasswordList = append(userPasswordList, password)
		userLimiters[index] = ratelimit.NewLimiters(user.DownloadMbps, user.UploadMbps)
		connLimiters[index] = connlimit.New(user.MaxIPs, user.MaxConnections)
	}
	service.UpdateUsers(userList, userPasswordList)
	inbound.service = service
	inbound.userNameList = userNameList
	inbound.userLimiters = userLimiters
	inbound.connLimiters = connLimiters
	inbound.accountGuard = accountquota.Global(ctx, logger)
	return inbound, nil
}

func (h *Inbound) NewConnectionEx(ctx context.Context, conn net.Conn, source M.Socksaddr, destination M.Socksaddr, onClose N.CloseHandlerFunc) {
	ctx = log.ContextWithNewID(ctx)
	var metadata adapter.InboundContext
	metadata.Inbound = h.Tag()
	metadata.InboundType = h.Type()
	//nolint:staticcheck
	metadata.InboundDetour = h.listener.ListenOptions().Detour
	//nolint:staticcheck
	metadata.OriginDestination = h.listener.UDPAddr()
	metadata.Source = source
	metadata.Destination = destination
	h.logger.InfoContext(ctx, "inbound connection from ", metadata.Source)
	userID, _ := auth.UserFromContext[int](ctx)
	user := h.userNameList[userID]
	if user != "" {
		metadata.User = user
		h.logger.InfoContext(ctx, "[", user, "] inbound connection to ", metadata.Destination)
	} else {
		user = F.ToString(userID)
		h.logger.InfoContext(ctx, "inbound connection to ", metadata.Destination)
	}
	if l := h.userLimiters[userID]; l != nil {
		metadata.DownloadRateLimiter = l.Download
		metadata.UploadRateLimiter = l.Upload
	}
	allowed, wrapped, reason := connlimit.CheckAndTrack(h.connLimiters[userID], h.accountGuard, user, metadata.Source, onClose)
	if !allowed {
		h.logger.InfoContext(ctx, "[", user, "] connection rejected: ", reason)
		N.CloseOnHandshakeFailure(conn, onClose, os.ErrPermission)
		return
	}
	onClose = wrapped
	h.router.RouteConnectionEx(ctx, conn, metadata, onClose)
}

func (h *Inbound) NewPacketConnectionEx(ctx context.Context, conn N.PacketConn, source M.Socksaddr, destination M.Socksaddr, onClose N.CloseHandlerFunc) {
	ctx = log.ContextWithNewID(ctx)
	var metadata adapter.InboundContext
	metadata.Inbound = h.Tag()
	metadata.InboundType = h.Type()
	//nolint:staticcheck
	metadata.InboundDetour = h.listener.ListenOptions().Detour
	//nolint:staticcheck
	metadata.OriginDestination = h.listener.UDPAddr()
	metadata.Source = source
	metadata.Destination = destination
	h.logger.InfoContext(ctx, "inbound packet connection from ", metadata.Source)
	userID, _ := auth.UserFromContext[int](ctx)
	user := h.userNameList[userID]
	if user != "" {
		metadata.User = user
		h.logger.InfoContext(ctx, "[", user, "] inbound packet connection to ", metadata.Destination)
	} else {
		user = F.ToString(userID)
		h.logger.InfoContext(ctx, "inbound packet connection to ", metadata.Destination)
	}
	if l := h.userLimiters[userID]; l != nil {
		metadata.DownloadRateLimiter = l.Download
		metadata.UploadRateLimiter = l.Upload
	}
	allowed, wrapped, reason := connlimit.CheckAndTrack(h.connLimiters[userID], h.accountGuard, user, metadata.Source, onClose)
	if !allowed {
		h.logger.InfoContext(ctx, "[", user, "] packet connection rejected: ", reason)
		N.CloseOnHandshakeFailure(conn, onClose, os.ErrPermission)
		return
	}
	onClose = wrapped
	h.router.RoutePacketConnectionEx(ctx, conn, metadata, onClose)
}

func (h *Inbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	if h.tlsConfig != nil {
		err := h.tlsConfig.Start()
		if err != nil {
			return err
		}
	}
	packetConn, err := h.listener.ListenUDP()
	if err != nil {
		return err
	}
	return h.service.Start(packetConn)
}

func (h *Inbound) Close() error {
	return common.Close(
		h.listener,
		h.tlsConfig,
		common.PtrOrNil(h.service),
	)
}
