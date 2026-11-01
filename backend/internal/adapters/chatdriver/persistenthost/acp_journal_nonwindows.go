//go:build !windows

package persistenthost

import (
	"fmt"
	"os"
)

func openACPJournalFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("secure persistent ACP prompt journal: %w", err)
	}
	return file, nil
}
