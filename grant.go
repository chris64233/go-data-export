package dataexport

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// 下载授权相关的错误，调用方应使用 errors.Is 判定。
var (
	ErrGrantNotFound          = errors.New("download grant not found")
	ErrGrantConflict          = errors.New("download grant conflict: immutable fields differ")
	ErrGrantRevoked           = errors.New("download grant revoked")
	ErrGrantExpired           = errors.New("download grant expired")
	ErrGrantCompleted         = errors.New("download grant completed")
	ErrGrantTerminal          = errors.New("download grant already reached a terminal state")
	ErrManifestMismatch       = errors.New("manifest digest mismatch")
	ErrRecipientMismatch      = errors.New("recipient mismatch")
	ErrRangeOutOfBounds       = errors.New("shard range out of granted bounds")
	ErrShardAlreadyDownloaded = errors.New("shard range already downloaded or in flight")
	ErrDownloadLimitReached   = errors.New("download limit reached")
	ErrTicketNotFound         = errors.New("download ticket not found")
	ErrTicketUsed             = errors.New("download ticket already used")
	ErrTicketExpired          = errors.New("download ticket expired")
	ErrInvalidGrant           = errors.New("invalid download grant request")
)

// GrantStatus 是下载授权的生命周期状态。
type GrantStatus string

const (
	StatusActive    GrantStatus = "active"
	StatusCompleted GrantStatus = "completed"
	StatusRevoked   GrantStatus = "revoked"
	StatusExpired   GrantStatus = "expired"
)

// Terminal 报告状态是否为终态。终态之间不允许再迁移：
// 撤销、下载完成与过期扫描并发时只能形成一个终态。
func (s GrantStatus) Terminal() bool {
	return s != StatusActive
}

// ShardRange 表示分片区间 [Start, End)，从 0 开始编号。
type ShardRange struct {
	Start int
	End   int
}

func (r ShardRange) valid() bool {
	return r.Start >= 0 && r.End > r.Start
}

// contains 报告 r 是否完整覆盖 o。
func (r ShardRange) contains(o ShardRange) bool {
	return o.Start >= r.Start && o.End <= r.End
}

// overlaps 报告两个区间是否存在交集。
func (r ShardRange) overlaps(o ShardRange) bool {
	return r.Start < o.End && o.Start < r.End
}

func (r ShardRange) String() string {
	return fmt.Sprintf("[%d,%d)", r.Start, r.End)
}

// mergeRanges 将 n 合并进区间列表，重叠或相邻的区间会被合并，
// 返回保持有序且互不重叠的新列表。
func mergeRanges(ranges []ShardRange, n ShardRange) []ShardRange {
	merged := make([]ShardRange, 0, len(ranges)+1)
	merged = append(merged, ranges...)
	merged = append(merged, n)
	sort.Slice(merged, func(i, j int) bool { return merged[i].Start < merged[j].Start })

	out := merged[:0]
	for _, r := range merged {
		if len(out) == 0 || r.Start > out[len(out)-1].End {
			out = append(out, r)
			continue
		}
		if r.End > out[len(out)-1].End {
			out[len(out)-1].End = r.End
		}
	}
	return out
}

// coversAll 报告已合并的有序区间列表是否完整覆盖 r。
func coversAll(ranges []ShardRange, r ShardRange) bool {
	cursor := r.Start
	for _, d := range ranges {
		if d.Start > cursor {
			return false
		}
		if d.End > cursor {
			cursor = d.End
		}
		if cursor >= r.End {
			return true
		}
	}
	return cursor >= r.End
}

// CreateGrantRequest 是签发下载授权的入参。授权一旦签发，清单摘要、
// 接收方、可下载分片范围、有效期与最大下载次数即被固定，不可变更。
type CreateGrantRequest struct {
	ID             string
	ExportID       string
	ManifestDigest string
	Recipient      string
	Range          ShardRange
	MaxDownloads   int
	TTL            time.Duration
}

func (r CreateGrantRequest) validate() error {
	switch {
	case r.ID == "" || r.ExportID == "":
		return fmt.Errorf("%w: id and export id are required", ErrInvalidGrant)
	case r.ManifestDigest == "":
		return fmt.Errorf("%w: manifest digest is required", ErrInvalidGrant)
	case r.Recipient == "":
		return fmt.Errorf("%w: recipient is required", ErrInvalidGrant)
	case !r.Range.valid():
		return fmt.Errorf("%w: invalid shard range %s", ErrInvalidGrant, r.Range)
	case r.MaxDownloads <= 0:
		return fmt.Errorf("%w: max downloads must be positive", ErrInvalidGrant)
	case r.TTL <= 0:
		return fmt.Errorf("%w: ttl must be positive", ErrInvalidGrant)
	}
	return nil
}

// sameBinding 比较两份授权的固定绑定字段是否一致。
func (r CreateGrantRequest) sameBinding(g *grantState) bool {
	return g.exportID == r.ExportID &&
		g.manifestDigest == r.ManifestDigest &&
		g.recipient == r.Recipient &&
		g.grantedRange == r.Range &&
		g.maxDownloads == r.MaxDownloads
}

// DownloadRequest 是一次分片下载请求。请求必须同时匹配授权号、接收方、
// 清单摘要与分片范围；导出任务标识不由请求方提供，而是由授权固定，
// 因此请求无法换取其他任务的文件。
type DownloadRequest struct {
	GrantID        string
	Recipient      string
	ManifestDigest string
	Range          ShardRange
}

// DownloadTicket 是下载校验通过后签发的短时效下载凭证。
// URL 仅通过返回值交给请求方，禁止写入普通日志或审计记录。
type DownloadTicket struct {
	ID        string
	GrantID   string
	ExportID  string
	Range     ShardRange
	URL       string
	ExpiresAt time.Time
}

// Redacted 返回不含下载地址的凭证描述，用于普通日志。
func (t *DownloadTicket) Redacted() string {
	return fmt.Sprintf("ticket=%s grant=%s export=%s range=%s (url redacted)",
		t.ID, t.GrantID, t.ExportID, t.Range)
}

// GrantView 是授权的查询视图，包含状态、已下载范围、次数、撤销原因
// 与最后访问时间，不包含任何下载地址。
type GrantView struct {
	ID             string
	ExportID       string
	ManifestDigest string
	Recipient      string
	Range          ShardRange
	MaxDownloads   int
	ExpiresAt      time.Time
	Status         GrantStatus
	Downloaded     []ShardRange
	DownloadCount  int
	RevokeReason   string
	LastAccessAt   time.Time
}

// AuditEntry 是一条下载审计记录。已完成的下载即使授权随后被撤销或
// 过期，审计记录也会保留。记录中刻意不包含下载地址。
type AuditEntry struct {
	Timestamp   time.Time
	GrantID     string
	ExportID    string
	Recipient   string
	Range       ShardRange
	DownloadSeq int
}
