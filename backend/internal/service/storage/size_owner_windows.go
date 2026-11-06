//go:build windows

package storage

import (
	"context"
	"os"
	"runtime"
	"strings"

	"golang.org/x/sys/windows"
)

// ValidateWindowsSizeOwner compares trusted AO principal evidence with an
// already-held, caller-owned no-reparse file handle. It never opens a path,
// enumerates a directory, reads contents or closes the caller's handle.
//
// The trusted caller must acquire the handle without following reparse points
// and independently maintain qualified ancestor/name binding and all Root
// preconditions throughout this call. Native handle metadata cannot establish
// those namespace facts. Unknown qualifications must remain Unknown, not Yes.
// The caller must keep this file open and exclusively stable throughout the
// call. Two readbacks detect observed changes; they do not create an atomic
// snapshot or exclude an intervening change restored before readback.
// OWNER_BOUND is only this comparison under that contract; ProbeSize remains
// unsupported on Windows until the full traversal source is qualified.
func ValidateWindowsSizeOwner(ctx context.Context, expected WindowsSizeOwnerScope, source TrustedWindowsSizeOwnerSource, held *os.File) (WindowsSizeOwnerEvidence, error) {
	return validateWindowsSizeOwner(ctx, expected, source, held, readWindowsSizeOwner)
}

type windowsSizeOwnerObservation struct {
	path                        string
	owner                       WindowsOwnerSID
	volume, indexHigh, indexLow uint32
	attributes, links           uint32
}

func validateWindowsSizeOwner(ctx context.Context, expected WindowsSizeOwnerScope, source TrustedWindowsSizeOwnerSource, held *os.File,
	read func(*os.File) (windowsSizeOwnerObservation, error),
) (WindowsSizeOwnerEvidence, error) {
	// Missing injection is a source capability gap, before even handle access.
	if source == nil {
		return windowsOwnerGap(ProbeUnsupported)
	}
	if ctx.Err() != nil {
		return windowsOwnerGap(ProbeCanceled)
	}
	root := expected.Root
	if !validWindowsOwnerScope(expected) || held == nil {
		return windowsOwnerGap(ProbeInvalid)
	}
	claims, err := source(ctx, expected)
	if err != nil || len(claims) != 1 || claims[0].Scope != expected {
		return windowsOwnerGap(ProbeInvalid)
	}
	// Copy the claim: a readback source may reuse its output slice.
	claim := claims[0]
	owner, ok := parseWindowsOwnerSID(claim.OwnerSID)
	if !ok {
		return windowsOwnerGap(ProbeInvalid)
	}
	if ctx.Err() != nil {
		return windowsOwnerGap(ProbeCanceled)
	}
	first, err := read(held)
	if err != nil {
		return windowsOwnerGap(probeErrorCode(err))
	}
	if first.path != root.Path || !windowsOwnerMatches(first.owner, owner) {
		return windowsOwnerGap(ProbeUnsafe)
	}
	if ctx.Err() != nil {
		return windowsOwnerGap(ProbeCanceled)
	}
	claims, err = source(ctx, expected)
	if err != nil || len(claims) != 1 || claims[0] != claim {
		return windowsOwnerGap(ProbeChanged)
	}
	if ctx.Err() != nil {
		return windowsOwnerGap(ProbeCanceled)
	}
	second, err := read(held)
	if err != nil {
		return windowsOwnerGap(probeErrorCode(err))
	}
	if first != second {
		return windowsOwnerGap(ProbeChanged)
	}
	if ctx.Err() != nil {
		return windowsOwnerGap(ProbeCanceled)
	}
	return WindowsSizeOwnerEvidence{State: WindowsOwnerBound}, nil
}

func validWindowsOwnerScope(s WindowsSizeOwnerScope) bool {
	for _, value := range []string{string(s.SessionID), string(s.ProjectID), s.OwnerID} {
		if !validKey(value) || len(value) > 256 || strings.ContainsAny(value, "\r\n") {
			return false
		}
	}
	r := s.Root
	return s.Generation > 0 && r.Trusted == Yes && r.Canonical == Yes && r.Quiescent == Yes &&
		r.NoReparseOrSymlinks == Yes && r.AliasUnambiguous == Yes && boundedAbsolute(r.Path) && probePath(r.Path) &&
		!strings.Contains(r.Path, "~")
}

// ConvertStringSidToSid + IsValidSid + ConvertSidToStringSid enforce native
// syntax and exact canonical spelling. SDDL aliases, names, NULs, whitespace,
// leading zeros and native-parser extensions must not become alternate claims.
func parseWindowsOwnerSID(value WindowsOwnerSID) (*windows.SID, bool) {
	s := string(value)
	if len(s) < 5 || len(s) > 184 || !strings.HasPrefix(s, "S-1-") {
		return nil, false
	}
	for _, c := range s[4:] {
		if c != '-' && (c < '0' || c > '9') {
			return nil, false
		}
	}
	sid, err := windows.StringToSid(s)
	return sid, err == nil && sid != nil && sid.IsValid() && sid.String() == s
}

func windowsOwnerMatches(value WindowsOwnerSID, expected *windows.SID) bool {
	actual, ok := parseWindowsOwnerSID(value)
	return ok && actual.Equals(expected)
}

func readWindowsSizeOwner(file *os.File) (windowsSizeOwnerObservation, error) {
	handle := windows.Handle(file.Fd())
	defer runtime.KeepAlive(file)
	kind, err := windows.GetFileType(handle)
	if err != nil {
		return windowsSizeOwnerObservation{}, err
	}
	if kind != windows.FILE_TYPE_DISK {
		return windowsSizeOwnerObservation{}, probeFailure{ProbeUnsafe}
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return windowsSizeOwnerObservation{}, err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 ||
		(info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 && info.NumberOfLinks != 1) {
		return windowsSizeOwnerObservation{}, probeFailure{ProbeUnsafe}
	}
	sd, err := windows.GetSecurityInfo(handle, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION)
	if err != nil {
		return windowsSizeOwnerObservation{}, err
	}
	owner, _, err := sd.Owner()
	if err != nil || owner == nil || !owner.IsValid() {
		return windowsSizeOwnerObservation{}, probeFailure{ProbeUnsafe}
	}
	// Exact DOS spelling only; do not case-fold, expand 8.3 or normalize aliases.
	buffer := make([]uint16, 4101)
	n, err := windows.GetFinalPathNameByHandle(handle, &buffer[0], uint32(len(buffer)), 0)
	if err != nil {
		return windowsSizeOwnerObservation{}, err
	}
	if n == 0 || n >= uint32(len(buffer)) {
		return windowsSizeOwnerObservation{}, probeFailure{ProbeAlias}
	}
	path := windows.UTF16ToString(buffer[:n])
	if !strings.HasPrefix(path, `\\?\`) {
		return windowsSizeOwnerObservation{}, probeFailure{ProbeAlias}
	}
	return windowsSizeOwnerObservation{path: strings.TrimPrefix(path, `\\?\`), owner: WindowsOwnerSID(owner.String()),
		volume: info.VolumeSerialNumber, indexHigh: info.FileIndexHigh, indexLow: info.FileIndexLow,
		attributes: info.FileAttributes, links: info.NumberOfLinks}, nil
}
