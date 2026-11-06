//go:build !windows

package storage

import (
	"context"
	"errors"
	"testing"
)

func TestWindowsSizeOwnerUnsupportedBeforeIssuerOrHandleIO(t *testing.T) {
	source := func(context.Context, WindowsSizeOwnerScope) ([]WindowsSizeOwnerClaim, error) {
		t.Fatal("Windows issuer invoked on unsupported platform")
		return nil, nil
	}
	got, err := ValidateWindowsSizeOwner(context.Background(), WindowsSizeOwnerScope{}, source, nil)
	if !errors.Is(err, ErrSourceGap) || got.State != WindowsOwnerSourceGap || got.Diagnostic != ProbeUnsupported {
		t.Fatalf("Windows owner evidence fabricated: %+v, %v", got, err)
	}
}
