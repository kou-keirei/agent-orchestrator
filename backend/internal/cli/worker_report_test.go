package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/reportactor"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

// Missing caller authentication must stop before either writer or receipt lookup.
// The real store below is used only to inspect and seed disposable fixture state.
type unauthenticatedReportStore struct{ t *testing.T }

func (s unauthenticatedReportStore) AcceptWorkerReport(context.Context, domain.WorkerReport) (domain.WorkerReportReceipt, error) {
	s.t.Error("unauthenticated submission reached report writer")
	return domain.WorkerReportReceipt{}, domain.ErrWorkerReportActorBindingUnavailable
}

func (s unauthenticatedReportStore) GetWorkerReport(context.Context, string) (domain.WorkerReportReceipt, bool, error) {
	s.t.Error("unauthenticated submission reached receipt lookup")
	return domain.WorkerReportReceipt{}, false, domain.ErrWorkerReportActorBindingUnavailable
}

func reportFixture(t *testing.T) (*sqlite.Store, domain.WorkerReport) {
	t.Helper()
	s, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	if err := s.UpsertProject(ctx, domain.ProjectRecord{ID: "project", Path: "/disposable", RegisteredAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	w, err := s.CreateSession(ctx, domain.SessionRecord{ProjectID: "project", Kind: domain.KindWorker, Harness: domain.HarnessFake, Activity: domain.Activity{State: domain.ActivityActive}, CreatedAt: time.Now(), UpdatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	i := domain.WorkItemIdentity{ID: "work", AttemptID: "attempt", ProjectID: "project", Repository: domain.RootWorkspaceRepoName, ExecutionBaseSHA: strings.Repeat("a", 40), OriginalParentSessionID: "parent", OriginalWorkerSessionID: w.ID, OwnerSessionID: w.ID, OwnerGeneration: 1, WorktreePath: "/disposable/work", Branch: "ao/disposable"}
	if _, err := s.CreateWorkItem(ctx, store.WorkItemRecord{Identity: i, OriginalNativeSessionID: "native"}); err != nil {
		t.Fatal(err)
	}
	l, err := s.AcquireWorkItemLease(ctx, i, domain.WorkLeaseGrant{ID: "lease", Duration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("explicit\r\nreport 🐈"))
	return s, domain.WorkerReport{ID: "report", Version: 1, Type: "done", Identity: i, LeaseID: l.Grant.ID, LeaseRevision: l.Revision, WorkerSessionID: w.ID, NativeSessionID: "native", NativeTurnID: "turn", OriginalNativeSessionID: "native", Content: "explicit\r\nreport 🐈", Digest: hex.EncodeToString(sum[:])}
}

func TestWorkerReportCLIIngress(t *testing.T) {
	cfg := setConfigEnv(t)
	s, report := reportFixture(t)
	// This is a negative ingress fixture, not an authenticated native caller.
	submission, err := reportactor.NewAcceptance(reportactor.Unavailable{}, unauthenticatedReportStore{t: t})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(httpd.NewWorkerReportHandler(submission))
	defer srv.Close()
	writeRunFileFor(t, cfg, srv)
	submit := func(r domain.WorkerReport, file bool) (string, error) {
		t.Helper()
		body, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		path := "-"
		if file {
			path = filepath.Join(t.TempDir(), "report.json")
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
		}
		out, _, err := executeCLI(t, Deps{In: strings.NewReader(string(body)), ProcessAlive: func(int) bool { return true }}, "worker", "report", "--file", path)
		return out, err
	}
	requireRefusal := func(r domain.WorkerReport, file bool) {
		t.Helper()
		out, err := submit(r, file)
		if ExitCode(err) != 1 || !strings.Contains(err.Error(), "WORKER_REPORT_ACTOR_BINDING_UNAVAILABLE") || out != "" {
			t.Fatalf("expected actor-binding refusal without receipt: output=%q err=%v", out, err)
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*domain.WorkerReport)
	}{
		{"owner", func(r *domain.WorkerReport) { r.Identity.OwnerSessionID = "foreign"; r.WorkerSessionID = "foreign" }},
		{"generation", func(r *domain.WorkerReport) { r.Identity.OwnerGeneration++ }},
		{"attempt", func(r *domain.WorkerReport) { r.Identity.AttemptID = "foreign" }},
		{"lease", func(r *domain.WorkerReport) { r.LeaseID = "foreign" }},
		{"revision", func(r *domain.WorkerReport) { r.LeaseRevision++ }},
		{"digest", func(r *domain.WorkerReport) { r.Digest = strings.Repeat("0", 64) }},
		{"native", func(r *domain.WorkerReport) { r.NativeSessionID = "foreign" }},
		{"original-native", func(r *domain.WorkerReport) { r.OriginalNativeSessionID = "foreign" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrong := report
			tc.mutate(&wrong)
			requireRefusal(wrong, false)
			if _, err := s.AcceptWorkerReport(context.Background(), wrong); err == nil {
				t.Fatal("persistence accepted invalid report")
			}
			if _, found, err := s.GetWorkerReport(context.Background(), wrong.ID); err != nil || found {
				t.Fatalf("rejected report persisted: %v %v", found, err)
			}
		})
	}
	requireRefusal(report, true)
	requireRefusal(report, false)
	if _, found, err := s.GetWorkerReport(context.Background(), report.ID); err != nil || found {
		t.Fatalf("unauthenticated report persisted: %v %v", found, err)
	}
	if events, err := s.ListOrchestrationEvents(context.Background(), report.Identity.ProjectID, 10); err != nil || len(events) != 0 {
		t.Fatalf("unauthenticated outbox: %+v %v", events, err)
	}
	// Seed persistence directly so an exact retry cannot disclose an old receipt.
	// This setup does not authenticate a CLI caller or qualify positive ingress.
	receipt, err := s.AcceptWorkerReport(context.Background(), report)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Report != report || receipt.EventID == "" || receipt.AcceptedAtMS <= 0 {
		t.Fatalf("changed persistence envelope: %+v", receipt)
	}
	if retry, err := s.AcceptWorkerReport(context.Background(), report); err != nil || retry != receipt {
		t.Fatalf("persistence retry: %+v %v", retry, err)
	}
	requireRefusal(report, true)
	requireRefusal(report, false)
	for _, mutate := range []func(*domain.WorkerReport){func(r *domain.WorkerReport) { r.ID = "second-done" }, func(r *domain.WorkerReport) { r.NativeTurnID = "changed-turn" }} {
		wrong := report
		mutate(&wrong)
		requireRefusal(wrong, false)
		if _, err := s.AcceptWorkerReport(context.Background(), wrong); !errors.Is(err, domain.ErrWorkerReportConflict) {
			t.Fatalf("persistence conflict: %v", err)
		}
	}
	if got, found, err := s.GetWorkerReport(context.Background(), report.ID); err != nil || !found || got != receipt {
		t.Fatalf("refused retry changed persisted receipt: %+v %v %v", got, found, err)
	}
	events, err := s.ListOrchestrationEvents(context.Background(), report.Identity.ProjectID, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("atomic outbox: %+v %v", events, err)
	}
	e := events[0]
	if e.ID != receipt.EventID || e.Kind != domain.OrchestrationWorkerReport || e.State != domain.OrchestrationPending || !e.SubmittedAt.IsZero() || !e.AcknowledgedAt.IsZero() {
		t.Fatalf("unexpected delivery: %+v", e)
	}
	l, found, err := s.GetWorkItemLease(context.Background(), report.Identity.ID)
	if err != nil || !found || l.Revision != report.LeaseRevision || l.State != domain.WorkLeaseActive {
		t.Fatalf("lease changed: %+v %v", l, err)
	}
	if _, err := s.ReleaseWorkItemLease(context.Background(), report.Identity, l); err != nil {
		t.Fatal(err)
	}
	stale := report
	stale.ID = "after-release"
	stale.Type = "checkpoint"
	requireRefusal(stale, false)
	if _, err := s.AcceptWorkerReport(context.Background(), stale); err == nil {
		t.Fatal("persistence accepted released lease")
	}
}

func TestWorkerReportCLIParsing(t *testing.T) {
	setConfigEnv(t)
	for _, tc := range []struct {
		body string
		args []string
	}{
		{"", nil}, {"{}", []string{"--file", "-", "extra"}}, {"{", []string{"--file", "-"}},
		{"{} {}", []string{"--file", "-"}}, {`{"typo":1}`, []string{"--file", "-"}},
		{strings.Repeat(" ", (1<<20)+1), []string{"--file", "-"}},
	} {
		args := append([]string{"worker", "report"}, tc.args...)
		_, _, err := executeCLI(t, Deps{In: strings.NewReader(tc.body)}, args...)
		if ExitCode(err) != 2 {
			t.Fatalf("%v: expected usage error, got %v", args, err)
		}
	}
}
