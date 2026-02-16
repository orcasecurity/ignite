package source

import (
	"bytes"
	"fmt"
	"os/exec"

	containerderr "github.com/containerd/containerd/errdefs"
	log "github.com/sirupsen/logrus"
)

// TarExtract extracts all files from a source to a directory
func TarExtract(src Source, dir string, args ...string) error {
	args = append([]string{"-x", "-C", dir}, args...)
	tarCmd := exec.Command("tar", args...)
	reader, err := src.Reader()
	if err != nil {
		return err
	}
	defer reader.Close()

	var stderr bytes.Buffer
	tarCmd.Stdin = reader
	tarCmd.Stderr = &stderr
	if err := tarCmd.Start(); err != nil {
		return err
	}

	if err := tarCmd.Wait(); err != nil {
		if stderr.Len() > 0 {
			log.Errorf("TarExtract: tar stderr: %s", stderr.String())
			return fmt.Errorf("tar extract failed (stderr: %s): %v", bytes.TrimSpace(stderr.Bytes()), err)
		}
		return fmt.Errorf("tar extract failed: %v", err)
	}

	if err = src.Cleanup(); err != nil {
		// Ignore the cleanup error if the resource no longer exists.
		if !containerderr.IsNotFound(err) {
			return err
		}
	}
	return nil
}
