//go:build !linux

package storage

// In particular, Windows junction/reparse, owner SID and volume binding need
// native handle evidence. Lstat/file length is not a substitute. No paths are
// accessed on unsupported platforms, including Windows and Darwin.
func openSizeRoot(string) (sizeNode, error) {
	return nil, probeFailure{ProbeUnsupported}
}
