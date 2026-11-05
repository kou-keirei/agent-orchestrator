package reportactor_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/reportactor"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// verifierFunc is an injected ordering/error fixture, never a native issuer.
type verifierFunc func(context.Context, domain.WorkerReport) error

func (f verifierFunc) VerifyWorkerReportActor(ctx context.Context, r domain.WorkerReport) error {
	return f(ctx, r)
}

type acceptanceStore struct {
	accept func(context.Context, domain.WorkerReport) (domain.WorkerReportReceipt, error)
}

func (s acceptanceStore) AcceptWorkerReport(ctx context.Context, r domain.WorkerReport) (domain.WorkerReportReceipt, error) {
	return s.accept(ctx, r)
}

func (acceptanceStore) GetWorkerReport(context.Context, string) (domain.WorkerReportReceipt, bool, error) {
	panic("acceptance must not look up receipts before verification")
}

func TestAcceptanceHoldsWithoutIssuer(t *testing.T) {
	store := acceptanceStore{accept: func(context.Context, domain.WorkerReport) (domain.WorkerReportReceipt, error) {
		t.Fatal("missing issuance reached the writer")
		return domain.WorkerReportReceipt{}, nil
	}}
	missing, err := reportactor.NewAcceptance(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := reportactor.NewAcceptance(reportactor.Unavailable{}, store)
	if err != nil {
		t.Fatal(err)
	}
	for _, acceptance := range []*reportactor.Acceptance{{}, missing, explicit} {
		receipt, err := acceptance.SubmitWorkerReport(context.Background(), reportClaim())
		requireHold(t, err)
		if receipt != (domain.WorkerReportReceipt{}) {
			t.Fatalf("HOLD exposed receipt: %+v", receipt)
		}
	}
	if _, err := reportactor.NewAcceptance(nil, nil); err == nil {
		t.Fatal("missing writer accepted")
	}
}

func TestAcceptanceVerifiesEveryInvocationBeforeWriter(t *testing.T) {
	ctx := context.WithValue(context.Background(), struct{}{}, "untrusted-context-is-not-proof")
	report := reportClaim()
	want := domain.WorkerReportReceipt{Report: report, EventID: "event", AcceptedAtMS: 1}
	denied := fmt.Errorf("runtime binding: %w", domain.ErrWorkerReportActorBindingUnavailable)
	storeErr := errors.New("writer failure")
	var verificationErr, writerErr error
	verified, written := 0, 0
	acceptance, err := reportactor.NewAcceptance(verifierFunc(func(got context.Context, r domain.WorkerReport) error {
		if got != ctx || r != report {
			t.Fatal("verification invocation changed")
		}
		verified++
		return verificationErr
	}), acceptanceStore{accept: func(got context.Context, r domain.WorkerReport) (domain.WorkerReportReceipt, error) {
		if got != ctx || r != report || verified != written+1 {
			t.Fatal("writer ran without verification of this invocation")
		}
		written++
		return want, writerErr
	}})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if got, err := acceptance.SubmitWorkerReport(ctx, report); err != nil || got != want {
			t.Fatalf("fixture-verified submission: %+v %v", got, err)
		}
	}
	writerErr = storeErr
	if got, err := acceptance.SubmitWorkerReport(ctx, report); !errors.Is(err, storeErr) || got != (domain.WorkerReportReceipt{}) {
		t.Fatalf("writer error lost or exposed receipt: %+v %v", got, err)
	}
	verificationErr = denied
	if got, err := acceptance.SubmitWorkerReport(ctx, report); err != denied || got != (domain.WorkerReportReceipt{}) {
		t.Fatalf("denied retry exposed receipt or lost error: %+v %v", got, err)
	}
	if verified != 4 || written != 3 {
		t.Fatalf("invocations: verified=%d written=%d", verified, written)
	}
}

func TestAcceptanceCancellationAfterVerificationStopsWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	acceptance, err := reportactor.NewAcceptance(verifierFunc(func(context.Context, domain.WorkerReport) error {
		cancel()
		return nil
	}), acceptanceStore{accept: func(context.Context, domain.WorkerReport) (domain.WorkerReportReceipt, error) {
		t.Fatal("canceled invocation reached writer")
		return domain.WorkerReportReceipt{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := acceptance.SubmitWorkerReport(ctx, reportClaim()); !errors.Is(err, context.Canceled) || got != (domain.WorkerReportReceipt{}) {
		t.Fatalf("canceled submission: %+v %v", got, err)
	}
}
