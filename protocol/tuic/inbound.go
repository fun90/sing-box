package tuic

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
	"github.com/sagernet/sing-box/common/uot"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	qtls "github.com/sagernet/sing-quic"
	"github.com/sagernet/sing-quic/tuic"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/auth"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/gofrs/uuid/v5"
)

func RegisterInbound(registry *inbound.Registry) {
	inbound.Register[option.TUICInboundOptions](registry, C.TypeTUIC, NewInbound)
}

type Inbound struct {
	inbound.Adapter
	router       adapter.ConnectionRouterEx
	logger       log.ContextLogger
	listener     *listener.Listener
	tlsConfig    tls.ServerConfig
	server       *tuic.Service[int]
	userNameList []string
	userLimiters []*ratelimit.Limiters
	connLimiters []*connlimit.Limiter
	accountGuard *accountquota.Guard
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.TUICInboundOptions) (adapter.Inbound, error) {
	options.UDPFragmentDefault = true
	if options.TLS == nil || !options.TLS.Enabled {
		return nil, C.ErrTLSRequired
	}
	tlsConfig, err := tls.NewServer(ctx, logger, common.PtrValueOrDefault(options.TLS))
	if err != nil {
		return nil, err
	}
	inbound := &Inbound{
		Adapter: inbound.NewAdapter(C.TypeTUIC, tag),
		router:  uot.NewRouter(router, logger),
		logger:  logger,
		listener: listener.New(listener.Options{
			Context: ctx,
			Logger:  logger,
			Listen:  options.ListenOptions,
		}),
		tlsConfig: tlsConfig,
	}
	var udpTimeout time.Duration
	if options.UDPTimeout != 0 {
		udpTimeout = time.Duration(options.UDPTimeout)
	} else {
		udpTimeout = C.UDPTimeout
	}
	service, err := tuic.NewService[int](tuic.ServiceOptions{
		Context:   ctx,
		Logger:    logger,
		TLSConfig: tlsConfig,
		QUICOptions: qtls.QUICOptions{
			IdleTimeout:             options.IdleTimeout.Build(),
			KeepAlivePeriod:         options.KeepAlivePeriod.Build(),
			StreamReceiveWindow:     options.StreamReceiveWindow.Value(),
			ConnectionReceiveWindow: options.ConnectionReceiveWindow.Value(),
			MaxConcurrentStreams:    options.MaxConcurrentStreams,
			InitialPacketSize:       options.InitialPacketSize,
			DisablePathMTUDiscovery: options.DisablePathMTUDiscovery,
		},
		CongestionControl: options.CongestionControl,
		AuthTimeout:       time.Duration(options.AuthTimeout),
		ZeroRTTHandshake:  options.ZeroRTTHandshake,
		Heartbeat:         time.Duration(options.Heartbeat),
		UDPTimeout:        udpTimeout,
		Handler:           inbound,
	})
	if err != nil {
		return nil, err
	}
	var userList []int
	var userNameList []string
	var userUUIDList [][16]byte
	var userPasswordList []string
	userLimiters := make([]*ratelimit.Limiters, len(options.Users))
	connLimiters := make([]*connlimit.Limiter, len(options.Users))
	for index, user := range options.Users {
		if user.UUID == "" {
			return nil, E.New("missing uuid for user ", index)
		}
		userUUID, err := uuid.FromString(user.UUID)
		if err != nil {
			return nil, E.Cause(err, "invalid uuid for user ", index)
		}
		userList = append(userList, index)
		userNameList = append(userNameList, user.Name)
		userUUIDList = append(userUUIDList, userUUID)
		userPasswordList = append(userPasswordList, user.Password)
		userLimiters[index] = ratelimit.NewLimiters(user.DownloadMbps, user.UploadMbps)
		connLimiters[index] = connlimit.New(user.MaxIPs, user.MaxConnections)
	}
	service.UpdateUsers(userList, userUUIDList, userPasswordList)
	inbound.server = service
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
	return h.server.Start(packetConn)
}

func (h *Inbound) Close() error {
	return common.Close(
		h.listener,
		h.tlsConfig,
		common.PtrOrNil(h.server),
	)
}
