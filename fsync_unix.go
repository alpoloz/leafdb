//go:build !windows

package leafdb

import (
	"os"

	"golang.org/x/sys/unix"
)

func fdatasync(file *os.File) error {
	if file == nil {
		return nil
	}
	return unix.Fsync(int(file.Fd()))
}
