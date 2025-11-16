//go:build windows
// +build windows

package prky

import (
	"os"
	"syscall"
)

const (
	// Win32 constant: Direct-to-disk write without caching
	_FILE_FLAG_WRITE_THROUGH = 0x80000000
)

func openWal(path string) (*os.File, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}

	// Use WRITE_THROUGH + OPEN_ALWAYS to match os.O_APPEND semantics
	handle, err := syscall.CreateFile(
		p,
		syscall.GENERIC_WRITE|syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE,
		nil,
		syscall.OPEN_ALWAYS,
		syscall.FILE_ATTRIBUTE_NORMAL|_FILE_FLAG_WRITE_THROUGH,
		0,
	)
	if err != nil {
		return nil, err
	}

	f := os.NewFile(uintptr(handle), path)

	// Force append position like os.OpenFile(O_APPEND)
	if _, err := f.Seek(0, os.SEEK_END); err != nil {
		f.Close()
		return nil, err
	}

	return f, nil
}
