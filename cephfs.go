package cephfs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	gocephfs "github.com/ceph/go-ceph/cephfs"
	"github.com/spf13/afero"
)

type Fs struct {
	mount *gocephfs.MountInfo
}

type cephArgs struct {
	Name        string
	KeyringPath string
	ConfigPath  string
}

func getCephArgs() cephArgs {
	envCephArgs := os.Getenv("CEPH_ARGS")

	args := strings.Fields(envCephArgs)

	myArgs := cephArgs{}

	prefixes := map[string]*string{
		"-n=":     &myArgs.Name,
		"--name=": &myArgs.Name,
		"-c=":     &myArgs.ConfigPath,
		"-k=":     &myArgs.KeyringPath,
	}

	for _, arg := range args {
		for prefix, ptr := range prefixes {
			s, found := strings.CutPrefix(arg, prefix)
			if found {
				*ptr = s
				break
			}
		}
	}

	return myArgs
}

func NewCephFS() (*Fs, error) {
	args := getCephArgs()

	mountId, _ := strings.CutPrefix(args.Name, "client.")

	mount, err := gocephfs.CreateMountWithId(mountId)
	if err != nil {
		return nil, fmt.Errorf("failed to create cephfs mount with id %s: %w", mountId, err)
	}

	if args.ConfigPath != "" {
		if err := mount.ReadConfigFile(args.ConfigPath); err != nil {
			return nil, fmt.Errorf("failed to read ceph config at %s: %w", args.ConfigPath, err)
		}
	} else {
		if err := mount.ReadDefaultConfigFile(); err != nil {
			return nil, fmt.Errorf("failed to read default ceph config: %w", err)
		}
	}

	if err := mount.Mount(); err != nil {
		return nil, fmt.Errorf("failed to mount cephfs: %w", err)
	}

	return &Fs{mount}, nil
}

func ToAferoFS(cephfsys *gocephfs.MountInfo) *Fs {
	return &Fs{cephfsys}
}

func convertErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gocephfs.ErrNotExist) {
		return os.ErrNotExist
	}
	if strings.Contains(err.Error(), "ret=-17") {
		return os.ErrExist
	}
	return err
}

// filesystem struct

func (fs *Fs) Unmount() error {
	if err := fs.mount.Unmount(); err != nil {
		return fmt.Errorf("failed to unmount cephfs: %w", err)
	}
	if err := fs.mount.Release(); err != nil {
		return fmt.Errorf("failed to release cephfs: %w", err)
	}
	return nil
}

// Create creates a file in the filesystem, returning the file and an
// error, if any happens.
func (fs *Fs) Create(path string) (afero.File, error) {
	cfile, err := fs.mount.Open(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}
	return &File{cfile, fs.mount, path}, nil
}

// Mkdir creates a directory in the filesystem, return an error if any
// happens.
func (fs *Fs) Mkdir(path string, perm os.FileMode) error {
	if err := fs.mount.MakeDir(path, uint32(perm.Perm())); err != nil {
		err = convertErr(err)
		return fmt.Errorf("failed to create directory %s: %w", path, err)
	}
	return nil
}

// MkdirAll creates a directory path and all parents that does not exist
// yet.
func (fs *Fs) MkdirAll(path string, perm os.FileMode) error {
	return fs.mount.MakeDirs(path, uint32(perm.Perm()))
}

// Open opens a file, returning it or an error, if any happens.
func (fs *Fs) Open(path string) (afero.File, error) {
	cfile, err := fs.mount.Open(path, os.O_RDONLY, 0)
	if err != nil {
		/* return nil, fmt.Errorf("failed to open filepath %s: %w", path, err) */
		return nil, convertErr(err)
	}
	return &File{cfile, fs.mount, path}, nil
}

// OpenFile opens a file using the given flags and the given mode.
func (fs *Fs) OpenFile(path string, flag int, perm os.FileMode) (afero.File, error) {
	cfile, err := fs.mount.Open(path, flag, uint32(perm.Perm()))
	if err != nil {
		// i'm unsure whether we need to convert some of the errors
		/* if errors.Is(err, cephfs.ErrNotExist) {
			return nil, os.ErrNotExist
		} */
		return nil, err
	}
	return &File{cfile, fs.mount, path}, nil
}

// Remove removes a file identified by name, returning an error, if any
// happens.
func (fs *Fs) Remove(path string) error {
	return convertErr(fs.mount.Unlink(path))
}

func forDirItem(dir *gocephfs.Directory, callback func(*gocephfs.DirEntry) error) error {
	for {
		de, err := dir.ReadDir()
		if err != nil {
			return fmt.Errorf("failed to readdir: %w", err)
		}

		if de == nil {
			break
		}

		err = callback(de)
		if err != nil {
			return fmt.Errorf("DirEntry callback failed: %w", err)
		}
	}
	return nil
}

// RemoveAll removes a directory path and any children it contains. It
// does not fail if the path does not exist (return nil).
func (fs *Fs) RemoveAll(path string) error {

	stat, err := fs.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if !stat.IsDir() {
		err := fs.Remove(path)
		if err != nil {
			return fmt.Errorf("'RemoveAll' failed to remove file at path %s: %w", path, err)
		}
		return nil
	}

	dir, err := fs.mount.OpenDir(path)
	if err != nil {
		return fmt.Errorf("failed to open dir %s: %w", path, err)
	}
	defer dir.Close()

	err = forDirItem(dir, func(de *gocephfs.DirEntry) error {
		if name := de.Name(); name == "." || name == ".." {
			return nil
		}

		fullPath := path + "/" + de.Name()

		switch de.DType() {
		case gocephfs.DTypeDir:
			if err := fs.RemoveAll(fullPath); err != nil {
				return err
			}
			return nil
		case gocephfs.DTypeLnk, gocephfs.DTypeReg:
			if err := fs.mount.Unlink(fullPath); err != nil {
				return fmt.Errorf("failed to remove file %s: %w", fullPath, err)
			}
			return nil
		default:
			if err := fs.mount.Unlink(fullPath); err != nil {
				return fmt.Errorf("failed to remove unknown entry: %w", err)
			}
			return nil
		}
	})
	if err != nil {
		return err
	}

	if err := fs.mount.RemoveDir(path); err != nil {
		return err
	}

	return nil
}

// Rename renames a file.
func (fs *Fs) Rename(oldPath, newPath string) error {
	return fs.mount.Rename(oldPath, newPath)
}

// Stat returns a FileInfo describing the named file, or an error, if any
// happens.
func (fs *Fs) Stat(path string) (os.FileInfo, error) {
	stat, err := fs.mount.Statx(path, gocephfs.StatxBasicStats, 0)
	if err != nil {
		// the webdav library checks for the os.ErrNotExist error
		// without this fix, the rename function doesn't work properly
		if errors.Is(err, gocephfs.ErrNotExist) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	return &FileInfo{stat: stat, path: path}, nil
}

// The name of this FileSystem
func (fs *Fs) Name() string {
	return "CephFS"
}

// Chmod changes the mode of the named file to mode.
func (fs *Fs) Chmod(path string, mode os.FileMode) error {
	return fs.mount.Chmod(path, uint32(mode.Perm()))
}

// Chown changes the uid and gid of the named file.
func (fs *Fs) Chown(path string, uid int, gid int) error {
	return fs.mount.Chown(path, uint32(uid), uint32(gid))
}

// Chtimes changes the access and modification times of the named file
func (fs *Fs) Chtimes(path string, atime time.Time, mtime time.Time) error {
	return errors.New("not implemented")
}

// file implementation

type File struct {
	*gocephfs.File
	mount *gocephfs.MountInfo
	path  string
}

func (f *File) Name() string {
	return f.path
}

// create list of items from a dir. use a callback function to mutate the item before adding it to the list
func listFromDir[T any](file *File, count int, callback func(*gocephfs.DirEntry) (T, error)) ([]T, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if stat.IsDir() {
		dir, err := file.mount.OpenDir(file.path)
		if err != nil {
			return nil, fmt.Errorf("failed %w", err)
		}
		defer dir.Close()

		if count == 0 {
			count = -1
		}

		list := make([]T, 0)

		for count != 0 {
			de, err := dir.ReadDir()
			if err != nil {
				return list, fmt.Errorf("failed cephfs.ReadDir: %w", err)
			}
			if de == nil {
				return list, nil
			}

			item, err := callback(de)
			if err != nil {
				return list, fmt.Errorf("listFromDir callback failed: %w", err)
			}

			list = append(list, item)

			if count > 0 {
				count--
			}
		}

		return list, nil

	} else {
		return nil, errors.New("not a directory")
	}
}

// list items in directory
func (f *File) Readdir(count int) ([]os.FileInfo, error) {
	return listFromDir(f, count, func(de *gocephfs.DirEntry) (os.FileInfo, error) {
		fullPath := f.path + "/" + de.Name()
		stat, err := f.mount.Statx(fullPath, gocephfs.StatxBasicStats, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to statx file %v: %w", fullPath, err)
		}

		return &FileInfo{stat: stat, path: fullPath}, nil
	})
}

// list items in directory only by name
func (f *File) Readdirnames(n int) ([]string, error) {
	return listFromDir(f, n, func(de *gocephfs.DirEntry) (string, error) {
		return de.Name(), nil
	})
}

func (f *File) Stat() (os.FileInfo, error) {
	stat, err := f.Fstatx(gocephfs.StatxBasicStats, 0)
	if err != nil {
		return nil, err
	}
	return &FileInfo{stat: stat, path: f.path}, nil
}

func (f *File) WriteString(s string) (int, error) {
	return f.Write([]byte(s))
}

// implements os.FileInfo interface for CephFS.
type FileInfo struct {
	stat *gocephfs.CephStatx
	path string
}

func (info *FileInfo) Name() string {
	return filepath.Base(info.path)
}

func (info *FileInfo) Size() int64 {
	return int64(info.stat.Size)
}

func (info *FileInfo) Mode() os.FileMode {
	return toFileMode(info.stat.Mode)
}

func (info *FileInfo) ModTime() time.Time {
	return time.Unix(int64(info.stat.Mtime.Sec), int64(info.stat.Mtime.Nsec))
}

func (info *FileInfo) IsDir() bool {
	return info.Mode().IsDir()
}

func (info *FileInfo) Sys() interface{} {
	return info.stat
}

func toFileMode(mode uint16) os.FileMode {
	var fm = os.FileMode(mode & 0777)
	switch mode & syscall.S_IFMT {
	case syscall.S_IFBLK:
		fm |= os.ModeDevice
	case syscall.S_IFCHR:
		fm |= os.ModeDevice | os.ModeCharDevice
	case syscall.S_IFDIR:
		fm |= os.ModeDir
	case syscall.S_IFIFO:
		fm |= os.ModeNamedPipe
	case syscall.S_IFLNK:
		fm |= os.ModeSymlink
	case syscall.S_IFREG:
	// nothing to do
	case syscall.S_IFSOCK:
		fm |= os.ModeSocket
	}
	if mode&syscall.S_ISGID != 0 {
		fm |= os.ModeSetgid
	}
	if mode&syscall.S_ISUID != 0 {
		fm |= os.ModeSetuid
	}
	if mode&syscall.S_ISVTX != 0 {
		fm |= os.ModeSticky
	}
	return fm
}
