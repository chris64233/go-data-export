package dataexport

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestService(clock *fakeClock) *Service {
	return NewService(WithClock(clock.Now), WithTicketTTL(time.Minute))
}

func sampleGrant(id string) CreateGrantRequest {
	return CreateGrantRequest{
		ID:             id,
		ExportID:       "export-001",
		ManifestDigest: "sha256:manifest",
		Recipient:      "alice",
		Range:          ShardRange{Start: 0, End: 10},
		MaxDownloads:   5,
		TTL:            time.Hour,
	}
}

func mustCreate(t *testing.T, s *Service, req CreateGrantRequest) GrantView {
	t.Helper()
	view, err := s.CreateGrant(req)
	if err != nil {
		t.Fatalf("CreateGrant: %v", err)
	}
	return view
}

func mustRequest(t *testing.T, s *Service, req DownloadRequest) *DownloadTicket {
	t.Helper()
	ticket, err := s.RequestDownload(req)
	if err != nil {
		t.Fatalf("RequestDownload: %v", err)
	}
	return ticket
}

func downloadReq(grantID string, start, end int) DownloadRequest {
	return DownloadRequest{
		GrantID:        grantID,
		Recipient:      "alice",
		ManifestDigest: "sha256:manifest",
		Range:          ShardRange{Start: start, End: end},
	}
}

func TestCreateGrantIdempotentAndConflict(t *testing.T) {
	s := newTestService(newFakeClock())

	first := mustCreate(t, s, sampleGrant("g1"))
	if first.Status != StatusActive {
		t.Fatalf("status = %s, want active", first.Status)
	}

	again := mustCreate(t, s, sampleGrant("g1"))
	if !reflect.DeepEqual(again, first) {
		t.Fatalf("idempotent resubmit changed result: %+v vs %+v", again, first)
	}

	mutations := []CreateGrantRequest{
		func() CreateGrantRequest { r := sampleGrant("g1"); r.ManifestDigest = "sha256:other"; return r }(),
		func() CreateGrantRequest { r := sampleGrant("g1"); r.Recipient = "bob"; return r }(),
		func() CreateGrantRequest { r := sampleGrant("g1"); r.Range = ShardRange{Start: 0, End: 5}; return r }(),
	}
	for i, m := range mutations {
		if _, err := s.CreateGrant(m); !errors.Is(err, ErrGrantConflict) {
			t.Fatalf("mutation %d: err = %v, want ErrGrantConflict", i, err)
		}
	}
}

func TestExpiredGrantCannotBeReactivated(t *testing.T) {
	clock := newFakeClock()
	s := newTestService(clock)

	req := sampleGrant("g1")
	req.TTL = time.Minute
	mustCreate(t, s, req)

	clock.Advance(2 * time.Minute)
	if n := s.ExpireScan(); n != 1 {
		t.Fatalf("ExpireScan = %d, want 1", n)
	}

	view, err := s.CreateGrant(req)
	if err != nil {
		t.Fatalf("resubmit same grant: %v", err)
	}
	if view.Status != StatusExpired {
		t.Fatalf("status = %s, want expired (must not reactivate)", view.Status)
	}
}

func TestRequestDownloadBindingValidation(t *testing.T) {
	s := newTestService(newFakeClock())
	mustCreate(t, s, sampleGrant("g1"))

	cases := []struct {
		name string
		req  DownloadRequest
		want error
	}{
		{"unknown grant", downloadReq("ghost", 0, 1), ErrGrantNotFound},
		{"recipient mismatch", func() DownloadRequest { r := downloadReq("g1", 0, 1); r.Recipient = "bob"; return r }(), ErrRecipientMismatch},
		{"manifest mismatch", func() DownloadRequest { r := downloadReq("g1", 0, 1); r.ManifestDigest = "sha256:tampered"; return r }(), ErrManifestMismatch},
		{"range before grant", downloadReq("g1", -1, 1), ErrRangeOutOfBounds},
		{"range beyond grant", downloadReq("g1", 8, 11), ErrRangeOutOfBounds},
		{"empty range", downloadReq("g1", 3, 3), ErrRangeOutOfBounds},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.RequestDownload(tc.req); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestTicketBoundToGrantExport(t *testing.T) {
	s := newTestService(newFakeClock())
	mustCreate(t, s, sampleGrant("g1"))
	other := sampleGrant("g2")
	other.ExportID = "export-002"
	mustCreate(t, s, other)

	ticket := mustRequest(t, s, downloadReq("g1", 0, 2))
	if ticket.ExportID != "export-001" {
		t.Fatalf("ticket export = %s, want export-001", ticket.ExportID)
	}
	if !strings.Contains(ticket.URL, "/export-001/") || strings.Contains(ticket.URL, "export-002") {
		t.Fatalf("ticket URL not bound to grant export: %s", ticket.URL)
	}
}

func TestDuplicateDownloadRejected(t *testing.T) {
	s := newTestService(newFakeClock())
	mustCreate(t, s, sampleGrant("g1"))

	ticket := mustRequest(t, s, downloadReq("g1", 0, 2))

	if _, err := s.RequestDownload(downloadReq("g1", 0, 2)); !errors.Is(err, ErrShardAlreadyDownloaded) {
		t.Fatalf("in-flight duplicate: %v", err)
	}
	if _, err := s.RequestDownload(downloadReq("g1", 1, 3)); !errors.Is(err, ErrShardAlreadyDownloaded) {
		t.Fatalf("in-flight overlap: %v", err)
	}

	if _, err := s.ConfirmDownload(ticket.ID); err != nil {
		t.Fatalf("ConfirmDownload: %v", err)
	}
	if _, err := s.RequestDownload(downloadReq("g1", 0, 2)); !errors.Is(err, ErrShardAlreadyDownloaded) {
		t.Fatalf("confirmed duplicate: %v", err)
	}
	if _, err := s.ConfirmDownload(ticket.ID); !errors.Is(err, ErrTicketUsed) {
		t.Fatalf("reused ticket: %v", err)
	}
}

func TestDownloadLimitEnforced(t *testing.T) {
	s := newTestService(newFakeClock())
	req := sampleGrant("g1")
	req.MaxDownloads = 1
	mustCreate(t, s, req)

	ticket := mustRequest(t, s, downloadReq("g1", 0, 1))
	if _, err := s.RequestDownload(downloadReq("g1", 1, 2)); !errors.Is(err, ErrDownloadLimitReached) {
		t.Fatalf("in-flight limit: %v", err)
	}

	view, err := s.ConfirmDownload(ticket.ID)
	if err != nil {
		t.Fatalf("ConfirmDownload: %v", err)
	}
	if view.Status != StatusCompleted {
		t.Fatalf("status = %s, want completed", view.Status)
	}
	if _, err := s.RequestDownload(downloadReq("g1", 1, 2)); !errors.Is(err, ErrGrantCompleted) {
		t.Fatalf("after completion: %v", err)
	}
}

func TestRevokeConfirmRaceSingleTerminalState(t *testing.T) {
	for attempt := 0; attempt < 20; attempt++ {
		clock := newFakeClock()
		s := newTestService(clock)
		req := sampleGrant("g1")
		req.MaxDownloads = 5
		mustCreate(t, s, req)

		tickets := make([]*DownloadTicket, 5)
		for i := range tickets {
			tickets[i] = mustRequest(t, s, downloadReq("g1", i, i+1))
		}

		var wg sync.WaitGroup
		confirmed := make(chan error, 5)
		for _, ticket := range tickets {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				_, err := s.ConfirmDownload(id)
				confirmed <- err
			}(ticket.ID)
		}
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = s.Revoke("g1", "security incident")
		}()
		go func() {
			defer wg.Done()
			clock.Advance(2 * time.Hour)
			s.ExpireScan()
		}()
		wg.Wait()
		close(confirmed)

		succeeded := 0
		for err := range confirmed {
			if err == nil {
				succeeded++
				continue
			}
			if !errors.Is(err, ErrGrantRevoked) && !errors.Is(err, ErrGrantExpired) && !errors.Is(err, ErrTicketExpired) {
				t.Fatalf("unexpected confirm error: %v", err)
			}
		}

		view, err := s.GetGrant("g1")
		if err != nil {
			t.Fatalf("GetGrant: %v", err)
		}
		if !view.Status.Terminal() {
			t.Fatalf("status = %s, want terminal", view.Status)
		}
		if view.DownloadCount != succeeded {
			t.Fatalf("download count = %d, confirmed = %d", view.DownloadCount, succeeded)
		}

		audit := s.AuditLog()
		if len(audit) != succeeded {
			t.Fatalf("audit entries = %d, confirmed = %d", len(audit), succeeded)
		}
		for _, e := range audit {
			if e.GrantID != "g1" || e.Recipient != "alice" {
				t.Fatalf("bad audit entry: %+v", e)
			}
		}

		if view.Status == StatusRevoked && view.RevokeReason != "security incident" {
			t.Fatalf("revoke reason = %q", view.RevokeReason)
		}
	}
}

func TestCompletedDownloadsKeepAuditAfterRevoke(t *testing.T) {
	s := newTestService(newFakeClock())
	mustCreate(t, s, sampleGrant("g1"))

	for _, r := range [][2]int{{0, 2}, {2, 4}} {
		ticket := mustRequest(t, s, downloadReq("g1", r[0], r[1]))
		if _, err := s.ConfirmDownload(ticket.ID); err != nil {
			t.Fatalf("ConfirmDownload: %v", err)
		}
	}

	view, err := s.Revoke("g1", "recipient offboarded")
	if err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if view.Status != StatusRevoked || view.RevokeReason != "recipient offboarded" {
		t.Fatalf("view = %+v", view)
	}
	if view.DownloadCount != 2 || len(view.Downloaded) != 1 || view.Downloaded[0] != (ShardRange{Start: 0, End: 4}) {
		t.Fatalf("downloaded state lost after revoke: %+v", view)
	}
	if got := len(s.AuditLog()); got != 2 {
		t.Fatalf("audit entries = %d, want 2", got)
	}

	if _, err := s.RequestDownload(downloadReq("g1", 4, 5)); !errors.Is(err, ErrGrantRevoked) {
		t.Fatalf("download after revoke: %v", err)
	}
	if _, err := s.Revoke("g1", "again"); err != nil {
		t.Fatalf("repeat revoke should be idempotent: %v", err)
	}
}

func TestExpireScanAndExpiredDownload(t *testing.T) {
	clock := newFakeClock()
	s := newTestService(clock)

	short := sampleGrant("g1")
	short.TTL = time.Minute
	mustCreate(t, s, short)
	mustCreate(t, s, sampleGrant("g2"))

	clock.Advance(2 * time.Minute)
	if n := s.ExpireScan(); n != 1 {
		t.Fatalf("ExpireScan = %d, want 1", n)
	}

	if _, err := s.RequestDownload(downloadReq("g1", 0, 1)); !errors.Is(err, ErrGrantExpired) {
		t.Fatalf("download on expired grant: %v", err)
	}
	if _, err := s.Revoke("g1", "too late"); !errors.Is(err, ErrGrantTerminal) {
		t.Fatalf("revoke on expired grant: %v", err)
	}

	if _, err := s.RequestDownload(downloadReq("g2", 0, 1)); err != nil {
		t.Fatalf("live grant rejected: %v", err)
	}
}

func TestExpiredTicketCannotConfirm(t *testing.T) {
	clock := newFakeClock()
	s := newTestService(clock)
	mustCreate(t, s, sampleGrant("g1"))

	ticket := mustRequest(t, s, downloadReq("g1", 0, 1))
	clock.Advance(2 * time.Minute)

	if _, err := s.ConfirmDownload(ticket.ID); !errors.Is(err, ErrTicketExpired) {
		t.Fatalf("confirm expired ticket: %v", err)
	}
}

func TestGrantViewAndAuditDoNotExposeURL(t *testing.T) {
	clock := newFakeClock()
	s := newTestService(clock)
	mustCreate(t, s, sampleGrant("g1"))

	before := clock.Now()
	ticket := mustRequest(t, s, downloadReq("g1", 0, 3))
	view, err := s.ConfirmDownload(ticket.ID)
	if err != nil {
		t.Fatalf("ConfirmDownload: %v", err)
	}

	if view.DownloadCount != 1 || len(view.Downloaded) != 1 {
		t.Fatalf("view = %+v", view)
	}
	if view.LastAccessAt.Before(before) {
		t.Fatalf("last access not updated: %v", view.LastAccessAt)
	}

	for _, haystack := range []string{
		fmt.Sprintf("%+v", view),
		fmt.Sprintf("%+v", s.AuditLog()),
		ticket.Redacted(),
	} {
		if strings.Contains(haystack, "token=") || strings.Contains(haystack, "downloads.internal") {
			t.Fatalf("download URL leaked into log-safe output: %s", haystack)
		}
	}
}
