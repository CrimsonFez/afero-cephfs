package cephfs

import (
	"errors"
	"fmt"
	"io"
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
	return &File{fs.mount, path, cfile, nil}, nil
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
	return fs.OpenFile(path, os.O_RDONLY, 0)
}

// OpenFile opens a file using the given flags and the given mode.
func (fs *Fs) OpenFile(path string, flag int, perm os.FileMode) (afero.File, error) {
	cfile, err := fs.mount.Open(path, flag, uint32(perm.Perm()))
	if err != nil {
		return nil, convertErr(err)
	}

	info, err := cfile.Fstatx(gocephfs.StatxBasicStats, 0)
	if err != nil {
		return nil, err
	}

	if toFileMode(info.Mode).IsDir() {
		dir, err := fs.mount.OpenDir(path)
		if err != nil {
			return nil, convertErr(err)
		}
		return &File{fs.mount, path, cfile, dir}, nil
	}

	return &File{fs.mount, path, cfile, nil}, nil
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
	mount *gocephfs.MountInfo
	path  string
	file  *gocephfs.File
	dir   *gocephfs.Directory
}

func (f *File) Name() string {
	return f.path
}

func (f *File) Close() error {
	var errs []error
	if f.file != nil {
		if err := f.file.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if f.dir != nil {
		if err := f.dir.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 1 {
		return fmt.Errorf("failed to close file for reasons; %w; %w", errs[0], errs[1])
	}
	if len(errs) == 1 {
		return errs[0]
	}
	return nil
}

var (
	ErrDirDoesntSupport = errors.New("type of Dir does not support this operation")
	ErrFileNil          = errors.New("cephfs file is nil, is this a directory?")
	ErrDirNil           = errors.New("cephfs dir is nil, is this a file?")
)

func (f *File) Read(buf []byte) (int, error) {
	if f.file == nil {
		return 0, ErrFileNil
	}
	return f.file.Read(buf)
}

func (f *File) ReadAt(buf []byte, offset int64) (int, error) {
	if f.file == nil {
		return 0, ErrFileNil
	}
	return f.file.ReadAt(buf, offset)
}

func (f *File) Write(buf []byte) (int, error) {
	if f.file == nil {
		return 0, ErrFileNil
	}
	return f.file.Write(buf)
}

func (f *File) WriteAt(buf []byte, off int64) (int, error) {
	if f.file == nil {
		return 0, ErrFileNil
	}
	return f.file.WriteAt(buf, off)
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	if f.file == nil {
		return 0, ErrFileNil
	}
	return f.file.Seek(offset, whence)
}

func (f *File) Stat() (os.FileInfo, error) {
	if f.file == nil {
		return nil, ErrFileNil
	}
	stat, err := f.file.Fstatx(gocephfs.StatxBasicStats, 0)
	if err != nil {
		return nil, err
	}
	return &FileInfo{stat: stat, path: f.path}, nil
}

func (f *File) Sync() error {
	if f.file == nil {
		return ErrFileNil
	}
	return f.file.Sync()
}

func (f *File) Truncate(size int64) error {
	if f.file == nil {
		return ErrFileNil
	}
	return f.file.Truncate(size)
}

func (f *File) WriteString(s string) (int, error) {
	if f.file == nil {
		return 0, ErrFileNil
	}
	return f.Write([]byte(s))
}

/*
os.File.Readdir spec:
Readdir reads the contents of the directory associated with file and returns a slice of up to n FileInfo values, as would be returned by Lstat, in directory order. Subsequent calls on the same file will yield further FileInfos.

If n > 0, Readdir returns at most n FileInfo structures. In this case, if Readdir returns an empty slice, it will return a non-nil error explaining why. At the end of a directory, the error is io.EOF.

If n <= 0, Readdir returns all the FileInfo from the directory in a single slice. In this case, if Readdir succeeds (reads all the way to the end of the directory), it returns the slice and a nil error. If it encounters an error before the end of the directory, Readdir returns the FileInfo read until that point and a non-nil error.

note:
cephfs does not have any restriction on reproducible ordering of directories. if we run into issues with this in the future we'll have to redo this function. That would likely involve having our own read itterator and instead of reading one file at a time, we read them all (-1) and sort them before culling the list to the requested ammount and returning
*/
func (f *File) Readdir(count int) ([]os.FileInfo, error) {
	if f.dir == nil {
		return nil, ErrDirNil
	}

	if count == 0 {
		count = -1
	}

	list := make([]os.FileInfo, 0)

	for {
		if count == 0 {
			return list, nil
		}
		de, err := f.dir.ReadDirPlus(gocephfs.StatxBasicStats, 0)
		if err != nil {
			return list, fmt.Errorf("cephfs: failed to list file: %w", err)
		}
		// de is nil at end of list
		if de == nil {
			// if we reached end of list before reaching zero, err is io.EOF
			if count > 0 {
				return list, io.EOF
			}
			return list, nil
		}

		// dont list the current dir and parent dir
		if name := de.Name(); name != "." && name != ".." {
			fullPath := f.path + "/" + de.Name()
			item := &FileInfo{stat: de.Statx(), path: fullPath}

			list = append(list, item)

			if count > 0 {
				count--
			}
		}
	}
}

// list items in directory only by name
func (f *File) Readdirnames(count int) ([]string, error) {
	deList, err := f.Readdir(count)
	list := make([]string, 0)
	for _, item := range deList {
		list = append(list, item.Name())
	}
	return list, err
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
