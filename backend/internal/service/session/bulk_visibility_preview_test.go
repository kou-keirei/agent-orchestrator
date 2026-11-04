package session

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func bulkVisibilityFixture(id string) BulkVisibilitySnapshot {
	return BulkVisibilitySnapshot{
		SessionID: id, ProjectID: "project", Kind: domain.KindWorker,
		Hidden: BulkVisibilityNo, RowRevision: "row-1", RowRevisionReadback: "row-1",
		CleanupRevision: "cleanup-1", CleanupRevisionReadback: "cleanup-1",
		Activity: domain.ActivityExited, Terminated: BulkVisibilityYes,
		PendingApprovals: BulkVisibilityNo, PendingTurns: BulkVisibilityNo,
		UnreconciledReports: BulkVisibilityNo, UnreconciledCI: BulkVisibilityNo,
		ProviderCertain: BulkVisibilityYes, OwnershipCertain: BulkVisibilityYes,
		Dirty: BulkVisibilityNo, Ambiguous: BulkVisibilityNo,
		Pinned: BulkVisibilityNo, UnderReview: BulkVisibilityNo,
	}
}

func TestBulkVisibilityPreview371Rows(t *testing.T) {
	// Hypothetical fixture facts only: these are not real session observations.
	var snapshots []BulkVisibilitySnapshot
	for i := 370; i >= 0; i-- {
		s := bulkVisibilityFixture(fmt.Sprintf("worker-%03d", i))
		if i >= 240 {
			s.Pinned = BulkVisibilityYes
		}
		snapshots = append(snapshots, s)
	}
	before := append([]BulkVisibilitySnapshot(nil), snapshots...)
	first, err := PreviewBulkVisibility("project", snapshots, 0)
	if err != nil {
		t.Fatal(err)
	}
	if first.EligibleTotal != 240 || first.BlockedTotal != 131 || first.AlreadyHiddenTotal != 0 || len(first.Eligible) != 200 || len(first.Blocked) != 0 || first.NextOffset != 200 {
		t.Fatalf("unexpected first page: %+v", first)
	}
	second, err := PreviewBulkVisibility("project", snapshots, first.NextOffset)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Eligible) != 40 || len(second.Blocked) != 131 || second.NextOffset != -1 {
		t.Fatalf("unexpected second page: %+v", second)
	}
	allEligible := append(first.Eligible, second.Eligible...)
	for i, entry := range allEligible {
		if entry.Snapshot.SessionID != fmt.Sprintf("worker-%03d", i) || len(entry.Reasons) != 0 {
			t.Fatalf("eligible order/reasons at %d: %+v", i, entry)
		}
	}
	for i, entry := range second.Blocked {
		if entry.Snapshot.SessionID != fmt.Sprintf("worker-%03d", i+240) || !reflect.DeepEqual(entry.Reasons, []BulkVisibilityReason{{Field: "pinned", Code: BulkVisibilityProtected}}) {
			t.Fatalf("blocked order/reasons at %d: %+v", i, entry)
		}
	}
	if !reflect.DeepEqual(snapshots, before) {
		t.Fatal("preview mutated input")
	}
	for i, j := 0, len(snapshots)-1; i < j; i, j = i+1, j-1 {
		snapshots[i], snapshots[j] = snapshots[j], snapshots[i]
	}
	again, err := PreviewBulkVisibility("project", snapshots, 0)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatal("preview depends on input ordering")
	}
	first.Eligible[0].Snapshot.RowRevision = "changed-output"
	if !reflect.DeepEqual(snapshots[0], before[len(before)-1]) {
		t.Fatal("output aliases input facts")
	}
}

func TestBulkVisibilityPreviewEvidence(t *testing.T) {
	// Every boolean fact is tested as missing, invalid, and explicitly unsafe.
	for _, fact := range []struct {
		name, field string
		safe        BulkVisibilityEvidence
	}{
		{"Hidden", "hidden", BulkVisibilityNo},
		{"Terminated", "terminated", BulkVisibilityYes},
		{"PendingApprovals", "pending_approvals", BulkVisibilityNo},
		{"PendingTurns", "pending_turns", BulkVisibilityNo},
		{"UnreconciledReports", "unreconciled_reports", BulkVisibilityNo},
		{"UnreconciledCI", "unreconciled_ci", BulkVisibilityNo},
		{"ProviderCertain", "provider_certain", BulkVisibilityYes},
		{"OwnershipCertain", "ownership_certain", BulkVisibilityYes},
		{"Dirty", "dirty", BulkVisibilityNo},
		{"Ambiguous", "ambiguous", BulkVisibilityNo},
		{"Pinned", "pinned", BulkVisibilityNo},
		{"UnderReview", "under_review", BulkVisibilityNo},
	} {
		unsafe := BulkVisibilityYes
		if fact.safe == BulkVisibilityYes {
			unsafe = BulkVisibilityNo
		}
		for _, check := range []struct {
			value BulkVisibilityEvidence
			code  BulkVisibilityReasonCode
		}{
			{BulkVisibilityUnknown, BulkVisibilityMissingEvidence},
			{BulkVisibilityEvidence(255), BulkVisibilityInvalidEvidence},
			{unsafe, BulkVisibilityProtected},
		} {
			t.Run(fmt.Sprintf("%s/%d", fact.name, check.value), func(t *testing.T) {
				s := bulkVisibilityFixture("worker")
				reflect.ValueOf(&s).Elem().FieldByName(fact.name).SetUint(uint64(check.value))
				p, err := PreviewBulkVisibility("project", []BulkVisibilitySnapshot{s}, 0)
				if err != nil {
					t.Fatal(err)
				}
				if fact.name == "Hidden" && check.value == BulkVisibilityYes {
					if p.AlreadyHiddenTotal != 1 || p.EligibleTotal != 0 || len(p.AlreadyHidden[0].Reasons) != 0 {
						t.Fatalf("hidden classification: %+v", p)
					}
					return
				}
				want := []BulkVisibilityReason{{Field: fact.field, Code: check.code}}
				if len(p.Blocked) != 1 || p.EligibleTotal != 0 || !reflect.DeepEqual(p.Blocked[0].Reasons, want) || p.Blocked[0].Snapshot != s {
					t.Fatalf("expected %v, got %+v", want, p)
				}
			})
		}
	}
}

func TestBulkVisibilityPreviewMixedCategories(t *testing.T) {
	a, b, c := bulkVisibilityFixture("z"), bulkVisibilityFixture("b"), bulkVisibilityFixture("a")
	a.Activity = domain.ActivityIdle
	b.Dirty = BulkVisibilityYes
	c.Hidden = BulkVisibilityYes
	rows := []BulkVisibilitySnapshot{c, b, a}
	p, err := PreviewBulkVisibility("project", rows, 0)
	if err != nil || len(p.Eligible) != 1 || len(p.Blocked) != 1 || len(p.AlreadyHidden) != 1 || p.Eligible[0].Snapshot != a || p.Blocked[0].Snapshot != b || p.AlreadyHidden[0].Snapshot != c {
		t.Fatalf("mixed categories: %+v, %v", p, err)
	}
	tail, err := PreviewBulkVisibility("project", rows, 1)
	if err != nil || len(tail.Eligible) != 0 || len(tail.Blocked) != 1 || len(tail.AlreadyHidden) != 1 {
		t.Fatalf("mixed category offset: %+v, %v", tail, err)
	}
	// Invalid identity outside the first 200 must still reject the whole request.
	rows = nil
	for i := 0; i < 201; i++ {
		rows = append(rows, bulkVisibilityFixture(fmt.Sprintf("%04d", i)))
	}
	rows[200] = rows[0]
	if _, err := PreviewBulkVisibility("project", rows, 0); err == nil {
		t.Fatal("duplicate beyond first page accepted")
	}
	rows[200].SessionID = ""
	if _, err := PreviewBulkVisibility("project", rows, 0); err == nil {
		t.Fatal("blank ID beyond first page accepted")
	}
}

func TestBulkVisibilityPreviewRevisionsAndProtection(t *testing.T) {
	for _, check := range []struct {
		name, field string
		change      func(*BulkVisibilitySnapshot)
		code        BulkVisibilityReasonCode
	}{
		{"row changed", "row_revision", func(s *BulkVisibilitySnapshot) { s.RowRevisionReadback = "row-2" }, BulkVisibilityRevisionChanged},
		{"cleanup changed", "cleanup_revision", func(s *BulkVisibilitySnapshot) { s.CleanupRevisionReadback = "cleanup-2" }, BulkVisibilityRevisionChanged},
		{"row absent", "row_revision", func(s *BulkVisibilitySnapshot) { s.RowRevision = "" }, BulkVisibilityMissingEvidence},
		{"row readback absent", "row_revision", func(s *BulkVisibilitySnapshot) { s.RowRevisionReadback = "" }, BulkVisibilityMissingEvidence},
		{"cleanup absent", "cleanup_revision", func(s *BulkVisibilitySnapshot) { s.CleanupRevision = " " }, BulkVisibilityMissingEvidence},
		{"cleanup readback absent", "cleanup_revision", func(s *BulkVisibilitySnapshot) { s.CleanupRevisionReadback = "" }, BulkVisibilityMissingEvidence},
		{"project absent", "project", func(s *BulkVisibilitySnapshot) { s.ProjectID = "" }, BulkVisibilityMissingEvidence},
		{"foreign project", "project", func(s *BulkVisibilitySnapshot) { s.ProjectID = "other" }, BulkVisibilityProjectMismatch},
		{"orchestrator", "kind", func(s *BulkVisibilitySnapshot) { s.Kind = domain.KindOrchestrator }, BulkVisibilityProtected},
		{"kind absent", "kind", func(s *BulkVisibilitySnapshot) { s.Kind = "" }, BulkVisibilityMissingEvidence},
		{"kind invalid", "kind", func(s *BulkVisibilitySnapshot) { s.Kind = "future-kind" }, BulkVisibilityInvalidEvidence},
		{"active", "activity", func(s *BulkVisibilitySnapshot) { s.Activity = domain.ActivityActive }, BulkVisibilityProtected},
		{"waiting", "activity", func(s *BulkVisibilitySnapshot) { s.Activity = domain.ActivityWaitingInput }, BulkVisibilityProtected},
		{"blocked", "activity", func(s *BulkVisibilitySnapshot) { s.Activity = domain.ActivityBlocked }, BulkVisibilityProtected},
		{"activity absent", "activity", func(s *BulkVisibilitySnapshot) { s.Activity = "" }, BulkVisibilityMissingEvidence},
		{"activity invalid", "activity", func(s *BulkVisibilitySnapshot) { s.Activity = "future-state" }, BulkVisibilityInvalidEvidence},
	} {
		t.Run(check.name, func(t *testing.T) {
			s := bulkVisibilityFixture("worker")
			check.change(&s)
			p, err := PreviewBulkVisibility("project", []BulkVisibilitySnapshot{s}, 0)
			want := []BulkVisibilityReason{{Field: check.field, Code: check.code}}
			if err != nil || len(p.Blocked) != 1 || p.EligibleTotal != 0 || !reflect.DeepEqual(p.Blocked[0].Reasons, want) || p.Blocked[0].Snapshot != s {
				t.Fatalf("expected %v and revision readback, got %+v, %v", want, p, err)
			}
		})
	}
	zero := BulkVisibilitySnapshot{SessionID: "unknown"}
	p, err := PreviewBulkVisibility("project", []BulkVisibilitySnapshot{zero}, 0)
	if err != nil || len(p.Blocked) != 1 || len(p.Blocked[0].Reasons) != 17 {
		t.Fatalf("missing facts must remain blocked: %+v, %v", p, err)
	}
	hidden := bulkVisibilityFixture("hidden")
	hidden.Hidden, hidden.OwnershipCertain, hidden.Pinned = BulkVisibilityYes, BulkVisibilityUnknown, BulkVisibilityYes
	p, err = PreviewBulkVisibility("project", []BulkVisibilitySnapshot{hidden}, 0)
	want := []BulkVisibilityReason{{Field: "ownership_certain", Code: BulkVisibilityMissingEvidence}, {Field: "pinned", Code: BulkVisibilityProtected}}
	if err != nil || len(p.AlreadyHidden) != 1 || p.EligibleTotal != 0 || !reflect.DeepEqual(p.AlreadyHidden[0].Reasons, want) {
		t.Fatalf("hidden row must preserve blockers: %+v, %v", p, err)
	}
}

func TestBulkVisibilityPreviewInputAndPageBoundaries(t *testing.T) {
	s := bulkVisibilityFixture("worker")
	for _, check := range []struct {
		name, project string
		rows          []BulkVisibilitySnapshot
		offset        int
	}{
		{"project missing", " ", nil, 0},
		{"negative offset", "project", nil, -1},
		{"past end", "project", []BulkVisibilitySnapshot{s}, 2},
		{"duplicate", "project", []BulkVisibilitySnapshot{s, s}, 0},
		{"duplicate at end offset", "project", []BulkVisibilitySnapshot{s, s}, 2},
		{"blank ID", "project", []BulkVisibilitySnapshot{{SessionID: " "}}, 0},
		{"padded ID", "project", []BulkVisibilitySnapshot{{SessionID: " worker"}}, 0},
	} {
		t.Run(check.name, func(t *testing.T) {
			p, err := PreviewBulkVisibility(check.project, check.rows, check.offset)
			if err == nil || !reflect.DeepEqual(p, BulkVisibilityPreview{NextOffset: -1}) {
				t.Fatalf("expected whole-request rejection: %+v, %v", p, err)
			}
		})
	}
	for _, size := range []int{0, 1, 199, 200, 201, 400, 401} {
		for _, category := range []string{"eligible", "blocked", "hidden"} {
			t.Run(fmt.Sprintf("%s/%d", category, size), func(t *testing.T) {
				var rows []BulkVisibilitySnapshot
				for i := 0; i < size; i++ {
					r := bulkVisibilityFixture(fmt.Sprintf("%04d", i))
					if category == "blocked" {
						r.OwnershipCertain = BulkVisibilityUnknown
					}
					if category == "hidden" {
						r.Hidden = BulkVisibilityYes
					}
					rows = append(rows, r)
				}
				seen := 0
				for offset := 0; ; {
					p, err := PreviewBulkVisibility("project", rows, offset)
					count := len(p.Eligible) + len(p.Blocked) + len(p.AlreadyHidden)
					if err != nil || count > 200 || (offset < size && count == 0) {
						t.Fatalf("invalid page: %+v, %v", p, err)
					}
					seen += count
					if p.NextOffset == -1 {
						break
					}
					if p.NextOffset != offset+200 {
						t.Fatal("non-progressing page")
					}
					offset = p.NextOffset
				}
				if seen != size {
					t.Fatalf("got %d rows, want %d", seen, size)
				}
				end, err := PreviewBulkVisibility("project", rows, size)
				if err != nil || len(end.Eligible)+len(end.Blocked)+len(end.AlreadyHidden) != 0 || end.NextOffset != -1 {
					t.Fatalf("invalid end page: %+v, %v", end, err)
				}
			})
		}
	}
}
