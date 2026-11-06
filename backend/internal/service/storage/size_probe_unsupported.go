//go:build !linux

package storage

// Windows is deliberately still unsupported: SizeProbeRequest supplies only a
// trusted numeric OwnerUID, with no trusted mapping to a Windows owner SID/ACL.
// Neither the current token, a SID's RID nor ownership read from the target may
// substitute for that missing caller authority. Even a native handle/length
// observation cannot qualify an AO-owned root under the present contract.
//
// A future Windows source also needs qualified handle-relative no-reparse
// traversal, ancestor/name binding, volume/file identity, bounded enumeration
// and alias/mutation checks. Lstat or path-based CreateFile alone is insufficient.
// Keep Windows in this build constraint until that entire source is qualified.
// No paths are accessed on unsupported platforms, including Windows and Darwin.
func openSizeRoot(string) (sizeNode, error) {
	return nil, probeFailure{ProbeUnsupported}
}
