package dataexport

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

const defaultTicketTTL = 60 * time.Second

// grantState 是授权的内部可变状态，所有字段仅在 Service.mu 保护下访问。
type grantState struct {
	exportID       string
	manifestDigest string
	recipient      string
	grantedRange   ShardRange
	maxDownloads   int
	expiresAt      time.Time
	createdAt      time.Time

	status        GrantStatus
	downloaded    []ShardRange
	inFlight      []ShardRange
	downloadCount int
	revokeReason  string
	lastAccessAt  time.Time
}

func (g *grantState) view(id string) GrantView {
	downloaded := make([]ShardRange, len(g.downloaded))
	copy(downloaded, g.downloaded)
	return GrantView{
		ID:             id,
		ExportID:       g.exportID,
		ManifestDigest: g.manifestDigest,
		Recipient:      g.recipient,
		Range:          g.grantedRange,
		MaxDownloads:   g.maxDownloads,
		ExpiresAt:      g.expiresAt,
		Status:         g.status,
		Downloaded:     downloaded,
		DownloadCount:  g.downloadCount,
		RevokeReason:   g.revokeReason,
		LastAccessAt:   g.lastAccessAt,
	}
}

// activeError 将非活跃状态映射为对应的错误。
func (g *grantState) activeError() error {
	switch g.status {
	case StatusActive:
		return nil
	case StatusRevoked:
		return ErrGrantRevoked
	case StatusExpired:
		return ErrGrantExpired
	case StatusCompleted:
		return ErrGrantCompleted
	default:
		return ErrGrantTerminal
	}
}

type ticketState struct {
	ticket DownloadTicket
	used   bool
}

// Option 用于定制 Service 行为，主要用于测试。
type Option func(*Service)

// WithClock 替换时间源，便于测试过期与并发场景。
func WithClock(now func() time.Time) Option {
	return func(s *Service) { s.now = now }
}

// WithTicketTTL 设置下载凭证的有效期。
func WithTicketTTL(ttl time.Duration) Option {
	return func(s *Service) { s.ticketTTL = ttl }
}

// Service 提供导出结果分片下载授权的签发、校验、确认、撤销与过期扫描。
// 所有状态迁移都在同一把互斥锁下串行完成，保证并发时只形成一个终态。
type Service struct {
	mu        sync.Mutex
	now       func() time.Time
	ticketTTL time.Duration

	grants  map[string]*grantState
	tickets map[string]*ticketState
	audit   []AuditEntry
}

// NewService 创建一个下载授权服务。
func NewService(opts ...Option) *Service {
	s := &Service{
		now:       time.Now,
		ticketTTL: defaultTicketTTL,
		grants:    make(map[string]*grantState),
		tickets:   make(map[string]*ticketState),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// CreateGrant 签发下载授权。相同授权号重复提交且固定字段一致时返回原
// 结果（包括已经过期或撤销的状态，过期授权不会被重新激活）；清单摘要、
// 接收方或分片范围等固定字段发生变化时返回 ErrGrantConflict。
func (s *Service) CreateGrant(req CreateGrantRequest) (GrantView, error) {
	if err := req.validate(); err != nil {
		return GrantView{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.grants[req.ID]; ok {
		if !req.sameBinding(existing) {
			return GrantView{}, fmt.Errorf("%w: grant %s", ErrGrantConflict, req.ID)
		}
		s.refreshLocked(existing)
		return existing.view(req.ID), nil
	}

	now := s.now()
	g := &grantState{
		exportID:       req.ExportID,
		manifestDigest: req.ManifestDigest,
		recipient:      req.Recipient,
		grantedRange:   req.Range,
		maxDownloads:   req.MaxDownloads,
		expiresAt:      now.Add(req.TTL),
		createdAt:      now,
		status:         StatusActive,
		lastAccessAt:   now,
	}
	s.grants[req.ID] = g
	return g.view(req.ID), nil
}

// RequestDownload 校验下载请求并签发短时效下载凭证。授权号、接收方、
// 清单摘要与分片范围必须同时匹配授权，任何一项不一致都会被拒绝。
func (s *Service) RequestDownload(req DownloadRequest) (*DownloadTicket, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	g, ok := s.grants[req.GrantID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrGrantNotFound, req.GrantID)
	}
	s.refreshLocked(g)
	g.lastAccessAt = s.now()

	if err := g.activeError(); err != nil {
		return nil, err
	}
	if req.Recipient != g.recipient {
		return nil, fmt.Errorf("%w: grant %s", ErrRecipientMismatch, req.GrantID)
	}
	if req.ManifestDigest != g.manifestDigest {
		return nil, fmt.Errorf("%w: grant %s", ErrManifestMismatch, req.GrantID)
	}
	if !req.Range.valid() || !g.grantedRange.contains(req.Range) {
		return nil, fmt.Errorf("%w: %s not within %s", ErrRangeOutOfBounds, req.Range, g.grantedRange)
	}
	for _, r := range g.downloaded {
		if r.overlaps(req.Range) {
			return nil, fmt.Errorf("%w: %s", ErrShardAlreadyDownloaded, req.Range)
		}
	}
	for _, r := range g.inFlight {
		if r.overlaps(req.Range) {
			return nil, fmt.Errorf("%w: %s", ErrShardAlreadyDownloaded, req.Range)
		}
	}
	if g.downloadCount+len(g.inFlight) >= g.maxDownloads {
		return nil, fmt.Errorf("%w: grant %s", ErrDownloadLimitReached, req.GrantID)
	}

	ts := &ticketState{ticket: DownloadTicket{
		ID:        newTicketID(),
		GrantID:   req.GrantID,
		ExportID:  g.exportID,
		Range:     req.Range,
		URL:       fmt.Sprintf("https://downloads.internal/%s/%d-%d?token=%s", g.exportID, req.Range.Start, req.Range.End, newTicketID()),
		ExpiresAt: s.now().Add(s.ticketTTL),
	}}
	s.tickets[ts.ticket.ID] = ts
	g.inFlight = append(g.inFlight, req.Range)

	t := ts.ticket
	return &t, nil
}

// ConfirmDownload 确认一次下载完成，记录审计并推进授权状态。若授权在
// 凭证签发后被撤销或过期，确认会被拒绝：终态只能形成一个，但已经确认
// 完成的下载审计记录始终保留。
func (s *Service) ConfirmDownload(ticketID string) (GrantView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ts, ok := s.tickets[ticketID]
	if !ok {
		return GrantView{}, fmt.Errorf("%w: %s", ErrTicketNotFound, ticketID)
	}
	if ts.used {
		return GrantView{}, fmt.Errorf("%w: %s", ErrTicketUsed, ticketID)
	}

	g := s.grants[ts.ticket.GrantID]
	s.refreshLocked(g)

	if !s.now().Before(ts.ticket.ExpiresAt) {
		s.releaseInFlightLocked(g, ts.ticket.Range)
		delete(s.tickets, ticketID)
		return GrantView{}, fmt.Errorf("%w: %s", ErrTicketExpired, ticketID)
	}
	if err := g.activeError(); err != nil {
		s.releaseInFlightLocked(g, ts.ticket.Range)
		delete(s.tickets, ticketID)
		return GrantView{}, err
	}

	ts.used = true
	s.releaseInFlightLocked(g, ts.ticket.Range)

	g.downloaded = mergeRanges(g.downloaded, ts.ticket.Range)
	g.downloadCount++
	g.lastAccessAt = s.now()
	s.audit = append(s.audit, AuditEntry{
		Timestamp:   s.now(),
		GrantID:     ts.ticket.GrantID,
		ExportID:    g.exportID,
		Recipient:   g.recipient,
		Range:       ts.ticket.Range,
		DownloadSeq: g.downloadCount,
	})

	if g.downloadCount >= g.maxDownloads || coversAll(g.downloaded, g.grantedRange) {
		g.status = StatusCompleted
	}
	return g.view(ts.ticket.GrantID), nil
}

// Revoke 撤销授权并记录原因。重复撤销返回当前状态；授权已处于其他
// 终态时返回 ErrGrantTerminal，保证终态唯一。
func (s *Service) Revoke(grantID, reason string) (GrantView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	g, ok := s.grants[grantID]
	if !ok {
		return GrantView{}, fmt.Errorf("%w: %s", ErrGrantNotFound, grantID)
	}
	s.refreshLocked(g)

	switch g.status {
	case StatusRevoked:
		return g.view(grantID), nil
	case StatusActive:
		g.status = StatusRevoked
		g.revokeReason = reason
		g.inFlight = nil
		return g.view(grantID), nil
	default:
		return GrantView{}, fmt.Errorf("%w: grant %s is %s", ErrGrantTerminal, grantID, g.status)
	}
}

// ExpireScan 扫描并标记所有已到期的授权，返回本次过期的数量。
// 与撤销、下载确认并发时同样只产生一个终态。
func (s *Service) ExpireScan() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	expired := 0
	for _, g := range s.grants {
		if g.status == StatusActive && !s.now().Before(g.expiresAt) {
			g.status = StatusExpired
			g.inFlight = nil
			expired++
		}
	}
	return expired
}

// GetGrant 返回授权的查询视图，包含状态、已下载范围、下载次数、
// 撤销原因与最后访问时间。
func (s *Service) GetGrant(grantID string) (GrantView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	g, ok := s.grants[grantID]
	if !ok {
		return GrantView{}, fmt.Errorf("%w: %s", ErrGrantNotFound, grantID)
	}
	s.refreshLocked(g)
	return g.view(grantID), nil
}

// AuditLog 返回全部下载审计记录的副本。
func (s *Service) AuditLog() []AuditEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]AuditEntry, len(s.audit))
	copy(out, s.audit)
	return out
}

// refreshLocked 实现惰性过期：读取状态时若已到期则迁移为过期终态。
func (s *Service) refreshLocked(g *grantState) {
	if g.status == StatusActive && !s.now().Before(g.expiresAt) {
		g.status = StatusExpired
		g.inFlight = nil
	}
}

func (s *Service) releaseInFlightLocked(g *grantState, r ShardRange) {
	for i, f := range g.inFlight {
		if f == r {
			g.inFlight = append(g.inFlight[:i], g.inFlight[i+1:]...)
			return
		}
	}
}

func newTicketID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
