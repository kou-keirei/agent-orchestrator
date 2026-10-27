package store

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

func TestRetentionPreviewProtectsMethodsAndIdentity(t *testing.T) {
	for _, tc := range []struct {
		method string
		reason string
	}{
		{"message.delta", "MESSAGE_DELTA_COVERAGE_UNPROVEN"},
		{"command.output.delta", "COMMAND_OUTPUT_RETAIN"},
		{"reasoning.delta", "REASONING_RECOVERABILITY_UNPROVEN"},
		{"turn.completed", "METHOD_NOT_ADMITTED_RETAIN"},
		{"unknown.method", "METHOD_NOT_ADMITTED_RETAIN"},
		{"", "METHOD_NOT_ADMITTED_RETAIN"},
	} {
		for _, id := range []string{"", "event-1", " "} {
			t.Run(tc.method+"/"+id, func(t *testing.T) {
				row := gen.ConversationProviderEvent{
					ID: 7, ConversationID: "conversation", BranchID: "branch",
					ProviderEventID: id, Method: tc.method,
					PayloadJson: `{"turnId":"settled","itemId":"item","state":"completed","checkpoint":true}`,
					ReceivedAt:  time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
				}
				before := row
				p := PreviewRetentionEvent(row)
				if p.Class != "UNKNOWN_REVIEW_REQUIRED" || p.StopReason != "SOURCE_GAP" || p.CoverageComplete {
					t.Fatalf("unproven fixture gained eligibility or coverage: %+v", p)
				}
				if p.DeletionAuthority != "NONE" || p.FilesystemReclaimEstimate != "UNKNOWN" || p.PolicyVersion != "ao16.retention-preview.v1" || p.EventID != 7 {
					t.Fatalf("wrong preview contract: %+v", p)
				}
				if !slices.Contains(p.Reasons, tc.reason) || !slices.IsSorted(p.Reasons) {
					t.Fatalf("missing or unstable protection reasons: %v", p.Reasons)
				}
				identified := RetentionNo
				if id != "" {
					identified = RetentionYes
				}
				if p.Identified != identified || slices.Contains(p.Reasons, "IDENTIFIED_EVENT_RETAIN") != (id != "") {
					t.Fatalf("lost dedupe protection: %+v", p)
				}
				facts := []RetentionFact{
					p.EventConversationBinding, p.EventBranchBinding, p.EventProviderTurnItemBinding,
					p.TurnSettled, p.CompleteProjection, p.CoveringCheckpoint,
					p.NativeReplayNeeded, p.NativeAcknowledged, p.BranchReplayNeeded, p.HostReplayNeeded,
					p.ReviewerObligation, p.ProvenanceObligation, p.RecoveryObligation,
					p.WithinExplicitPolicyWindow,
				}
				for _, fact := range facts {
					if fact != RetentionUnknown {
						t.Fatalf("row metadata or claimed payload promoted an unsupported fact: %q", fact)
					}
				}
				if row != before || !reflect.DeepEqual(p, PreviewRetentionEvent(row)) {
					t.Fatal("preview mutated input or was nondeterministic")
				}
			})
		}
	}
}

func TestRetentionPreviewBoundedAndIndependentOfPayloadAndAge(t *testing.T) {
	row := gen.ConversationProviderEvent{ID: 1, Method: "message.delta"}
	want := PreviewRetentionEvent(row)
	// Deliberately exceed the tested 4 KiB report bound at the input boundary.
	// The kernel never parses or returns this content, even malformed JSON.
	row.PayloadJson = strings.Repeat("secret", 40000)
	row.ConversationID = row.PayloadJson
	row.BranchID = row.PayloadJson
	row.ReceivedAt = time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)
	got := PreviewRetentionEvent(row)
	if !reflect.DeepEqual(want, got) {
		t.Fatal("unverified content, association labels or age changed eligibility")
	}
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > 4096 || strings.Contains(string(b), "secret") {
		t.Fatalf("unbounded or payload-bearing report: %d bytes", len(b))
	}
	got.Reasons[0] = "caller mutation"
	if !reflect.DeepEqual(want, PreviewRetentionEvent(row)) {
		t.Fatal("caller mutation leaked into subsequent results")
	}
}
