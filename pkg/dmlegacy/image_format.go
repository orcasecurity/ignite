package dmlegacy

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	api "github.com/weaveworks/ignite/pkg/apis/ignite"
	"github.com/weaveworks/ignite/pkg/constants"
	"github.com/weaveworks/ignite/pkg/source"
	"github.com/weaveworks/ignite/pkg/util"
)

const (
	blockSize       = 4096   // Block size to use for the ext4 filesystems, this is the default
	// defaultMinimumBaseSizeGB is the default floor (in GB) for the base image when IGNITE_BASE_IMAGE_MIN_SIZE_GB is unset.
	defaultMinimumBaseSizeGB = 10
	baseImageSizeMultiplier  = 5 // multiplier over OCI size (extraction + fs overhead)
)

// env var to override minimum base image size (in GB). E.g. IGNITE_BASE_IMAGE_MIN_SIZE_GB=15 for huge images.
const minimumBaseSizeGBEnv = "IGNITE_BASE_IMAGE_MIN_SIZE_GB"

func getMinimumBaseSizeBytes() int64 {
	gb := defaultMinimumBaseSizeGB
	if s := os.Getenv(minimumBaseSizeGBEnv); s != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && n >= 1 && n <= 100 {
			gb = n
		}
	}
	return int64(gb) * 1024 * 1024 * 1024
}

// CreateImageFilesystem creates an ext4 filesystem in a file, containing the files from the source
func CreateImageFilesystem(img *api.Image, src source.Source) error {
	log.Debugf("Allocating image file and formatting it with ext4...")
	p := path.Join(img.ObjectPath(), constants.IMAGE_FS)
	imageFile, err := os.Create(p)
	if err != nil {
		errMsg := errors.Wrapf(err, "failed to create image file for %s", img.GetUID())
		log.Errorf("image import: %v", errMsg)
		return errMsg
	}
	defer imageFile.Close()

	// To accommodate space for the tar contents and the ext4 journal + metadata,
	// make the base image a sparse file. OCI image "size" is often compressed/layer size;
	// extracted content can be much larger, so we use a multiplier and a minimum (default 10 GB, overridable via IGNITE_BASE_IMAGE_MIN_SIZE_GB).
	// The file will be shrunk by resizeToMinimum later.
	minimumBaseSizeBytes := getMinimumBaseSizeBytes()
	minimumBaseSizeGB := minimumBaseSizeBytes / (1024 * 1024 * 1024)
	log.Infof("image import: minimum base image size %d GB (override with IGNITE_BASE_IMAGE_MIN_SIZE_GB)", minimumBaseSizeGB)
	computedSize := int64(img.Status.OCISource.Size.Bytes()) * int64(baseImageSizeMultiplier)
	baseImageSize := computedSize
	if baseImageSize < minimumBaseSizeBytes {
		baseImageSize = minimumBaseSizeBytes
	}

	if err := imageFile.Truncate(baseImageSize); err != nil {
		errMsg := errors.Wrapf(err, "failed to allocate space for image %s", img.GetUID())
		log.Errorf("image import: %v", errMsg)
		return errMsg
	}

	// Use mkfs.ext4 to create the new image with an inode size of 256
	// (gexto doesn't support anything but 128, but as long as we're not using that it's fine)
	if _, err := util.ExecuteCommand("mkfs.ext4", "-b", strconv.Itoa(blockSize),
		"-I", "256", "-F", "-E", "lazy_itable_init=0,lazy_journal_init=0", p); err != nil {
		errMsg := errors.Wrapf(err, "failed to format image %s", img.GetUID())
		log.Errorf("image import mkfs.ext4 failed: %v", errMsg)
		return errMsg
	}

	// Proceed with populating the image with files
	if err := addFiles(img, src); err != nil {
		log.Errorf("image import addFiles failed: %v", err)
		return err
	}

	// Resize the image to its minimum size
	if err := resizeToMinimum(img); err != nil {
		log.Errorf("image import resizeToMinimum failed: %v", err)
		return err
	}
	return nil
}

// addFiles copies the contents of the tar file into the ext4 filesystem
func addFiles(img *api.Image, src source.Source) (err error) {
	log.Debugf("Copying in files to the image file from a source...")
	p := path.Join(img.ObjectPath(), constants.IMAGE_FS)
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		return
	}
	defer os.RemoveAll(tempDir)

	if _, err := util.ExecuteCommand("mount", "-o", "loop", p, tempDir); err != nil {
		errMsg := fmt.Errorf("failed to mount image %q: %v", p, err)
		log.Errorf("image import mount failed: %v", errMsg)
		return errMsg
	}
	defer util.DeferErr(&err, func() error {
		_, execErr := util.ExecuteCommand("umount", tempDir)
		return execErr
	})

	err = source.TarExtract(src, tempDir)
	if err != nil {
		log.Errorf("image import TarExtract failed: %v", err)
		return
	}

	err = setupResolvConf(tempDir)
	if err != nil {
		log.Errorf("image import setupResolvConf failed: %v", err)
	}

	return
}

// setupResolvConf makes sure there is a resolv.conf file, otherwise
// name resolution won't work. The kernel uses DHCP by default, and
// puts the nameservers in /proc/net/pnp at runtime. Hence, as a default,
// if /etc/resolv.conf doesn't exist, we can use /proc/net/pnp as /etc/resolv.conf
func setupResolvConf(tempDir string) error {
	resolvConf := filepath.Join(tempDir, "/etc/resolv.conf")
	empty, err := util.FileIsEmpty(resolvConf)
	if err != nil {
		return err
	}

	if !empty {
		return nil
	}

	// Ensure /etc directory exists. Some images don't contain /etc directory
	// which results in symlink creation failure.
	if err := os.MkdirAll(filepath.Dir(resolvConf), constants.DATA_DIR_PERM); err != nil {
		return err
	}

	return os.Symlink("../proc/net/pnp", resolvConf)
}

// resizeToMinimum resizes the given image to the smallest size possible
func resizeToMinimum(img *api.Image) (err error) {
	p := path.Join(img.ObjectPath(), constants.IMAGE_FS)
	var minSize int64
	var imageFile *os.File

	if minSize, err = getMinSize(p); err != nil {
		log.Errorf("image import getMinSize failed: %v", err)
		return
	}

	if imageFile, err = os.OpenFile(p, os.O_RDWR, constants.DATA_DIR_FILE_PERM); err != nil {
		log.Errorf("image import OpenFile failed: %v", err)
		return
	}
	defer util.DeferErr(&err, imageFile.Close)

	minSizeBytes := minSize * blockSize

	log.Debugf("Truncating %q to %d bytes", p, minSizeBytes)
	if err = imageFile.Truncate(minSizeBytes); err != nil {
		err = fmt.Errorf("failed to shrink image %q: %v", img.GetUID(), err)
		log.Errorf("image import truncate failed: %v", err)
	}

	return
}

// getMinSize retrieves the minimum size for a block device file
// containing a filesystem and shrinks the filesystem to that size
func getMinSize(p string) (minSize int64, err error) {
	// Loop mount the image for resize2fs
	imageLoop, err := newLoopDev(p, false)
	if err != nil {
		log.Errorf("image import newLoopDev failed: %v", err)
		return
	}

	// Defer the detach
	defer util.DeferErr(&err, imageLoop.Detach)

	// Call e2fsck for resize2fs, it sometimes requires this
	// e2fsck throws an error if the filesystem gets repaired, so just ignore it
	_, _ = util.ExecuteCommand("e2fsck", "-p", "-f", imageLoop.Path())

	// Retrieve the minimum size for the filesystem
	log.Debugf("Retrieving minimum size for %q", imageLoop.Path())
	out, err := util.ExecuteCommand("resize2fs", "-P", imageLoop.Path())
	if err != nil {
		log.Errorf("image import resize2fs -P failed: %v", err)
		return
	}

	if minSize, err = parseResize2fsOutputForMinSize(out); err != nil {
		log.Errorf("image import parseResize2fs output failed: %v", err)
		return
	}

	log.Debugf("Minimum size: %d blocks", minSize)

	// Perform the filesystem resize
	_, err = util.ExecuteCommand("resize2fs", imageLoop.Path(), strconv.FormatInt(minSize, 10))
	if err != nil {
		log.Errorf("image import resize2fs shrink failed: %v", err)
	}
	return
}

// parseResize2fsOutputForMinSize extracts the trailing number from `resize2fs -P`
func parseResize2fsOutputForMinSize(out string) (int64, error) {
	// LANG=en_US.utf8
	//   resize2fs 1.45.3 (14-Jul-2019)
	//   Estimated minimum size of the filesystem: 5813528
	// LANG=zh_CN.utf8  https://github.com/tytso/e2fsprogs/blob/v1.45.4/po/zh_CN.po#L7240-L7241
	//   resize2fs 1.44.1 (24-Mar-2018)
	//   预计文件系统的最小尺寸：61817
	split := strings.FieldsFunc(out, func(r rune) bool {
		return unicode.IsPunct(r) || unicode.IsSpace(r)
	})
	return strconv.ParseInt(split[len(split)-1], 10, 64)
}
