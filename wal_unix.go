//go:build !windows
// +build !windows

package prky

import "os"

// openWal uses the standard Go file I/O path on non-Windows platforms.
// Could later be upgraded to O_DIRECT on Linux for even better fsync behavior.
func openWal(path string) (*os.File, error) {
    return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
}
