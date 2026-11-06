//go:build linux

package storage

import (
	"errors"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

type linuxSizeNode struct {
	file      *os.File
	reader    *os.File
	parent    int
	name      string
	initial   sizeMetadata
	ancestors []*linuxSizeNode
}

func openSizeRoot(path string) (sizeNode, error) {
	fd, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	anchor, err := newLinuxSizeNode(fd, -1, "")
	if err != nil {
		return nil, err
	}
	chain := []*linuxSizeNode{anchor}
	for _, name := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		child, openErr := chain[len(chain)-1].openChild(name, false)
		if openErr != nil {
			for _, node := range chain {
				_ = node.close()
			}
			return nil, openErr
		}
		chain = append(chain, child)
		if !child.metadata().Directory {
			for _, node := range chain {
				_ = node.close()
			}
			return nil, probeFailure{ProbeUnsafe}
		}
	}
	root := chain[len(chain)-1]
	root.ancestors = chain[:len(chain)-1]
	return root, nil
}

func newLinuxSizeNode(fd, parent int, name string) (*linuxSizeNode, error) {
	file := os.NewFile(uintptr(fd), "size-probe")
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		_ = file.Close()
		return nil, err
	}
	m := linuxSizeMetadata(stat)
	if !m.Directory && !m.Regular {
		_ = file.Close()
		return nil, probeFailure{ProbeUnsafe}
	}
	return &linuxSizeNode{file: file, parent: parent, name: name, initial: m}, nil
}

//nolint:unconvert // Linux Stat_t field widths differ across architectures.
func linuxSizeMetadata(s unix.Stat_t) sizeMetadata {
	return sizeMetadata{Device: uint64(s.Dev), Inode: s.Ino, Owner: s.Uid, Links: uint64(s.Nlink), Size: s.Size,
		Directory: s.Mode&unix.S_IFMT == unix.S_IFDIR, Regular: s.Mode&unix.S_IFMT == unix.S_IFREG,
		ModifiedSec: int64(s.Mtim.Sec), ModifiedNS: int64(s.Mtim.Nsec), ChangedSec: int64(s.Ctim.Sec), ChangedNS: int64(s.Ctim.Nsec)}
}

func (n *linuxSizeNode) metadata() sizeMetadata { return n.initial }

func (n *linuxSizeNode) names(limit int) ([]string, error) {
	if n.reader == nil {
		// Reopen the held directory, never its potentially replaced path.
		fd, err := unix.Openat(int(n.file.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err != nil {
			return nil, err
		}
		n.reader = os.NewFile(uintptr(fd), "size-probe-directory")
	}
	return n.reader.Readdirnames(limit)
}

func (n *linuxSizeNode) open(name string) (sizeNode, error) {
	return n.openChild(name, true)
}

func (n *linuxSizeNode) openChild(name string, rejectMount bool) (*linuxSizeNode, error) {
	// O_PATH avoids opening device drivers/FIFOs for I/O. Symlinks themselves
	// may be held, but are rejected by fstat without ever following the target.
	// NO_XDEV also rejects bind mounts with the same st_dev below the AO root.
	resolve := uint64(unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS)
	if rejectMount {
		resolve |= unix.RESOLVE_NO_XDEV
	}
	fd, err := unix.Openat2(int(n.file.Fd()), name, &unix.OpenHow{Flags: unix.O_PATH | unix.O_NOFOLLOW | unix.O_CLOEXEC, Resolve: resolve})
	if err != nil {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) {
			return nil, probeFailure{ProbeUnsupported}
		}
		if errors.Is(err, unix.EXDEV) || errors.Is(err, unix.ELOOP) {
			return nil, probeFailure{ProbeUnsafe}
		}
		return nil, err
	}
	return newLinuxSizeNode(fd, int(n.file.Fd()), name)
}

func (n *linuxSizeNode) check() error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(n.file.Fd()), &stat); err != nil {
		return err
	}
	if linuxSizeMetadata(stat) != n.initial {
		return probeFailure{ProbeChanged}
	}
	if n.parent >= 0 {
		if err := unix.Fstatat(n.parent, n.name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return err
		}
		if linuxSizeMetadata(stat) != n.initial {
			return probeFailure{ProbeChanged}
		}
	}
	for _, ancestor := range n.ancestors {
		if err := ancestor.check(); err != nil {
			return err
		}
	}
	return nil
}

func (n *linuxSizeNode) close() error {
	err := n.file.Close()
	if n.reader != nil {
		if closeErr := n.reader.Close(); err == nil {
			err = closeErr
		}
	}
	for _, ancestor := range n.ancestors {
		if closeErr := ancestor.close(); err == nil {
			err = closeErr
		}
	}
	return err
}
