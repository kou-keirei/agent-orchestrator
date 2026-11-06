//go:build !windows

package storage

import (
	"context"
	"os"
)

// ValidateWindowsSizeOwner leaves Windows ownership unknown on other platforms.
// It never invokes the issuer, accesses a path/handle or changes ProbeSize.
func ValidateWindowsSizeOwner(context.Context, WindowsSizeOwnerScope, TrustedWindowsSizeOwnerSource, *os.File) (WindowsSizeOwnerEvidence, error) {
	return windowsOwnerGap(ProbeUnsupported)
}
