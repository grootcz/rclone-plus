package hybridzfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/xattr"

	"github.com/rclone/rclone/fs/config/configstruct"

	"github.com/rclone/rclone/fs/hash"

	"github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/lib/encoder"
)

const metadataTimeFormat = time.RFC3339Nano

// system metadata keys which this backend owns
//
// not all values supported on all OSes
var systemMetadataInfo = map[string]fs.MetadataHelp{
	"mode": {
		Help:    "File type and mode",
		Type:    "octal, unix style",
		Example: "0100664",
	},
	"uid": {
		Help:    "User ID of owner",
		Type:    "decimal number",
		Example: "500",
	},
	"gid": {
		Help:    "Group ID of owner",
		Type:    "decimal number",
		Example: "500",
	},
	"rdev": {
		Help:    "Device ID (if special file)",
		Type:    "hexadecimal",
		Example: "1abc",
	},
	"atime": {
		Help:    "Time of last access",
		Type:    "RFC 3339",
		Example: "2006-01-02T15:04:05.999999999Z07:00",
	},
	"mtime": {
		Help:    "Time of last modification",
		Type:    "RFC 3339",
		Example: "2006-01-02T15:04:05.999999999Z07:00",
	},
	"btime": {
		Help:    "Time of file birth (creation)",
		Type:    "RFC 3339",
		Example: "2006-01-02T15:04:05.999999999Z07:00",
	},
}

var commandHelp = []fs.CommandHelp{
	{
		Name:  "noop",
		Short: "A null operation for testing backend commands",
		Long: `This is a test command which has some options
you can try to change the output.`,
		Opts: map[string]string{
			"echo":  "echo the input arguments",
			"error": "return an error based on option value",
		},
	},
}

const linkSuffix = ".rclonelink"

type timeType = fs.Enum[timeTypeChoices]

const (
	mTime timeType = iota
	aTime
	bTime
	cTime
)

type timeTypeChoices struct{}

func (timeTypeChoices) Choices() []string {
	return []string{
		mTime: "mtime",
		aTime: "atime",
		bTime: "btime",
		cTime: "ctime",
	}
}

// Register with Fs
func init() {
	fsi := &fs.RegInfo{
		Name:        "hybridzfs",
		Description: "Local VFS Disk",
		NewFs:       NewFs,
		CommandHelp: commandHelp,
		MetadataInfo: &fs.MetadataInfo{
			System: systemMetadataInfo,
			Help: `Depending on which OS is in use the local backend may return only some
of the system metadata. Setting system metadata is supported on all
OSes but setting user metadata is only supported on linux, freebsd,
netbsd, macOS and Solaris. It is **not** supported on Windows yet
([see pkg/attrs#47](https://github.com/pkg/xattr/issues/47)).

User metadata is stored as extended attributes (which may not be
supported by all file systems) under the "user.*" prefix.

Metadata is supported on files and directories.
`,
		},
		Options: []fs.Option{{
			Name:     "nounc",
			Help:     "Disable UNC (long path names) conversion on Windows.",
			Default:  false,
			Advanced: runtime.GOOS != "windows",
			Examples: []fs.OptionExample{{
				Value: "true",
				Help:  "Disables long file names.",
			}},
		}, {
			Name:     "copy_links",
			Help:     "Follow symlinks and copy the pointed to item.",
			Default:  false,
			NoPrefix: true,
			ShortOpt: "L",
			Advanced: true,
		}, {
			Name:     "links",
			Help:     "Translate symlinks to/from regular files with a '" + linkSuffix + "' extension.",
			Default:  false,
			NoPrefix: true,
			ShortOpt: "l",
			Advanced: true,
		}, {
			Name: "skip_links",
			Help: `Don't warn about skipped symlinks.

This flag disables warning messages on skipped symlinks or junction
points, as you explicitly acknowledge that they should be skipped.`,
			Default:  false,
			NoPrefix: true,
			Advanced: true,
		}, {
			Name: "zero_size_links",
			Help: `Assume the Stat size of links is zero (and read them instead) (deprecated).

Rclone used to use the Stat size of links as the link size, but this fails in quite a few places:

- Windows
- On some virtual filesystems (such ash LucidLink)
- Android

So rclone now always reads the link.
`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "unicode_normalization",
			Help: `Apply unicode NFC normalization to paths and filenames.

This flag can be used to normalize file names into unicode NFC form
that are read from the local filesystem.

Rclone does not normally touch the encoding of file names it reads from
the file system.

This can be useful when using macOS as it normally provides decomposed (NFD)
unicode which in some language (eg Korean) doesn't display properly on
some OSes.

Note that rclone compares filenames with unicode normalization in the sync
routine so this flag shouldn't normally be used.`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "no_check_updated",
			Help: `Don't check to see if the files change during upload.

Normally rclone checks the size and modification time of files as they
are being uploaded and aborts with a message which starts "can't copy -
source file is being updated" if the file changes during upload.

However on some file systems this modification time check may fail (e.g.
[Glusterfs #2206](https://github.com/rclone/rclone/issues/2206)) so this
check can be disabled with this flag.

If this flag is set, rclone will use its best efforts to transfer a
file which is being updated. If the file is only having things
appended to it (e.g. a log) then rclone will transfer the log file with
the size it had the first time rclone saw it.

If the file is being modified throughout (not just appended to) then
the transfer may fail with a hash check failure.

In detail, once the file has had stat() called on it for the first
time we:

- Only transfer the size that stat gave
- Only checksum the size that stat gave
- Don't update the stat info for the file

**NB** do not use this flag on a Windows Volume Shadow (VSS). For some
unknown reason, files in a VSS sometimes show different sizes from the
directory listing (where the initial stat value comes from on Windows)
and when stat is called on them directly. Other copy tools always use
the direct stat value and setting this flag will disable that.
`,
			Default:  false,
			Advanced: true,
		}, {
			Name:     "one_file_system",
			Help:     "Don't cross filesystem boundaries (unix/macOS only).",
			Default:  false,
			NoPrefix: true,
			ShortOpt: "x",
			Advanced: true,
		}, {
			Name: "case_sensitive",
			Help: `Force the filesystem to report itself as case sensitive.

Normally the local backend declares itself as case insensitive on
Windows/macOS and case sensitive for everything else.  Use this flag
to override the default choice.`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "case_insensitive",
			Help: `Force the filesystem to report itself as case insensitive.

Normally the local backend declares itself as case insensitive on
Windows/macOS and case sensitive for everything else.  Use this flag
to override the default choice.`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "no_preallocate",
			Help: `Disable preallocation of disk space for transferred files.

Preallocation of disk space helps prevent filesystem fragmentation.
However, some virtual filesystem layers (such as Google Drive File
Stream) may incorrectly set the actual file size equal to the
preallocated space, causing checksum and file size checks to fail.
Use this flag to disable preallocation.`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "no_sparse",
			Help: `Disable sparse files for multi-thread downloads.

On Windows platforms rclone will make sparse files when doing
multi-thread downloads. This avoids long pauses on large files where
the OS zeros the file. However sparse files may be undesirable as they
cause disk fragmentation and can be slow to work with.`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "no_set_modtime",
			Help: `Disable setting modtime.

Normally rclone updates modification time of files after they are done
uploading. This can cause permissions issues on Linux platforms when 
the user rclone is running as does not own the file uploaded, such as
when copying to a CIFS mount owned by another user. If this option is 
enabled, rclone will no longer update the modtime after copying a file.`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "time_type",
			Help: `Set what kind of time is returned.

Normally rclone does all operations on the mtime or Modification time.

If you set this flag then rclone will return the Modified time as whatever
you set here. So if you use "rclone lsl --local-time-type ctime" then
you will see ctimes in the listing.

If the OS doesn't support returning the time_type specified then rclone
will silently replace it with the modification time which all OSes support.

- mtime is supported by all OSes
- atime is supported on all OSes except: plan9, js
- btime is only supported on: Windows, macOS, freebsd, netbsd
- ctime is supported on all Oses except: Windows, plan9, js

Note that setting the time will still set the modified time so this is
only useful for reading.
`,
			Default:  mTime,
			Advanced: true,
			Examples: []fs.OptionExample{{
				Value: mTime.String(),
				Help:  "The last modification time.",
			}, {
				Value: aTime.String(),
				Help:  "The last access time.",
			}, {
				Value: bTime.String(),
				Help:  "The creation time.",
			}, {
				Value: cTime.String(),
				Help:  "The last status change time.",
			}},
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default:  encoder.OS,
		}},
	}
	fs.Register(fsi)
}

// Options defines the configuration for this backend
type Options struct {
	FollowSymlinks    bool                 `config:"copy_links"`
	TranslateSymlinks bool                 `config:"links"`
	SkipSymlinks      bool                 `config:"skip_links"`
	UTFNorm           bool                 `config:"unicode_normalization"`
	NoCheckUpdated    bool                 `config:"no_check_updated"`
	NoUNC             bool                 `config:"nounc"`
	OneFileSystem     bool                 `config:"one_file_system"`
	CaseSensitive     bool                 `config:"case_sensitive"`
	CaseInsensitive   bool                 `config:"case_insensitive"`
	NoPreAllocate     bool                 `config:"no_preallocate"`
	NoSparse          bool                 `config:"no_sparse"`
	NoSetModTime      bool                 `config:"no_set_modtime"`
	TimeType          timeType             `config:"time_type"`
	Enc               encoder.MultiEncoder `config:"encoding"`
	NoClone           bool                 `config:"no_clone"`
}

type Hvfs struct {
	name           string              // the name of the remote
	root           string              // The root directory (OS path)
	opt            Options             // parsed config options
	features       *fs.Features        // optional features
	dev            uint64              // device number of root node
	precisionOk    sync.Once           // Whether we need to read the precision
	precision      time.Duration       // precision of local filesystem
	warnedMu       sync.Mutex          // used for locking access to 'warned'.
	warned         map[string]struct{} // whether we have warned about this string
	xattrSupported atomic.Int32        // whether xattrs are supported

	// do os.Lstat or os.Stat
	lstat        func(name string) (os.FileInfo, error)
	objectMetaMu sync.RWMutex // global lock for Object metadata

	localFs  fs.Fs // is same as cache vfs fs
	sqlFs    fs.Fs
	remoteFs fs.Fs
}

var (
	errLinksAndCopyLinks = errors.New("can't use -l/--links with -L/--copy-links")
	errLinksNeedsSuffix  = errors.New("need \"" + linkSuffix + "\" suffix to refer to symlink when using -l/--links")
)

const xattrSupported = xattr.XATTR_SUPPORTED

func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	if opt.TranslateSymlinks && opt.FollowSymlinks {
		return nil, errLinksAndCopyLinks
	}

	localFs, err := local.NewFs(ctx, name, root, m)
	if err != nil {
		return nil, err
	}

	f := &Hvfs{
		name:   name,
		opt:    *opt,
		warned: make(map[string]struct{}),
		dev:    devUnset,
		lstat:  os.Lstat,

		localFs: localFs,
	}
	if xattrSupported {
		f.xattrSupported.Store(1)
	}
	f.root = local.CleanRootPath(root, f.opt.NoUNC, f.opt.Enc)
	f.features = (&fs.Features{
		CaseInsensitive:          f.caseInsensitive(),
		CanHaveEmptyDirectories:  true,
		IsLocal:                  true,
		SlowHash:                 true,
		ReadMetadata:             true,
		WriteMetadata:            true,
		ReadDirMetadata:          true,
		WriteDirMetadata:         true,
		WriteDirSetModTime:       true,
		UserDirMetadata:          xattrSupported, // can only R/W general purpose metadata if xattrs are supported
		DirModTimeUpdatesOnWrite: true,
		UserMetadata:             xattrSupported, // can only R/W general purpose metadata if xattrs are supported
		FilterAware:              true,
		PartialUploads:           true,
		About:                    f.About,
	}).Fill(ctx, f)
	if opt.FollowSymlinks {
		f.lstat = os.Stat
	}
	if opt.FollowSymlinks {
		f.lstat = os.Stat
	}
	if opt.NoClone {
		// Disable server-side copy when --local-no-clone is set
		f.features.Copy = nil
	}

	// Check to see if this points to a file
	fi, err := f.lstat(f.root)
	if err == nil {
		f.dev = readDevice(fi, f.opt.OneFileSystem)
	}

	// Check to see if this is a .rclonelink if not found
	hasLinkSuffix := strings.HasSuffix(f.root, linkSuffix)
	if hasLinkSuffix && opt.TranslateSymlinks && os.IsNotExist(err) {
		fi, err = f.lstat(strings.TrimSuffix(f.root, linkSuffix))
	}
	if err == nil && f.isRegular(fi.Mode()) {
		// Handle the odd case, that a symlink was specified by name without the link suffix
		if !hasLinkSuffix && opt.TranslateSymlinks && fi.Mode()&os.ModeSymlink != 0 {
			return nil, errLinksNeedsSuffix
		}
		// It is a file, so use the parent as the root
		f.root = filepath.Dir(f.root)
		// return an error with an fs which points to the parent
		return f, fs.ErrorIsFile
	}

	fs.Logf(nil, "name=%s, root=%s", name, root)

	return f, nil
}

func (f *Hvfs) caseInsensitive() bool {
	if f.opt.CaseSensitive {
		return false
	}
	if f.opt.CaseInsensitive {
		return true
	}
	// FIXME not entirely accurate since you can have case
	// sensitive Fses on darwin and case insensitive Fses on linux.
	// Should probably check but that would involve creating a
	// file in the remote to be most accurate which probably isn't
	// desirable.
	return runtime.GOOS == "windows" || runtime.GOOS == "darwin"
}

func (f *Hvfs) isRegular(mode os.FileMode) bool {
	if !f.opt.TranslateSymlinks {
		return mode.IsRegular()
	}

	// fi.Mode().IsRegular() tests that all mode bits are zero
	// Since symlinks are accepted, test that all other bits are zero,
	// except the symlink bit
	return mode&os.ModeType&^os.ModeSymlink == 0
}

const devUnset = 0xdeadbeefcafebabf

func readDevice(fi os.FileInfo, oneFileSystem bool) uint64 {
	return devUnset
}

/************************************************** Hvfs ***************************************************/

// Name of the remote (as passed into NewFs)
func (f *Hvfs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Hvfs) Root() string {
	return f.opt.Enc.ToStandardPath(filepath.ToSlash(f.root))
}

// String converts this Fs to a string
func (f *Hvfs) String() string {
	return fmt.Sprintf("Local file system at %s", f.Root())
}

func (f *Hvfs) Precision() (precision time.Duration) {
	return f.localFs.Precision()
}

// Hashes returns the supported hash sets.
func (f *Hvfs) Hashes() hash.Set {
	return f.localFs.Hashes()
}

// Features returns the optional features of this Fs
func (f *Hvfs) Features() *fs.Features {
	return f.features
}

func (f *Hvfs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	return f.localFs.List(ctx, dir)
}

func (f *Hvfs) NewObject(ctx context.Context, remote string) (fs.Object, error) {

	return f.localFs.NewObject(ctx, remote)
}

func (f *Hvfs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	fs.Logf(nil, "[remote] Put %s", src.Remote())
	return f.localFs.Put(ctx, in, src, options...)
}

func (f *Hvfs) Mkdir(ctx context.Context, dir string) error {
	fs.Logf(nil, "[remote] Mkdir %s", dir)
	return f.localFs.Mkdir(ctx, dir)
}

func (f *Hvfs) Rmdir(ctx context.Context, dir string) error {
	fs.Logf(nil, "[remote] Rmdir %s", dir)
	return f.localFs.Rmdir(ctx, dir)
}

/*********************************************** fs.PutStreamer ******************************************************/

func (f *Hvfs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	putStreamer, ok := f.localFs.(fs.PutStreamer)
	if !ok {
		return nil, fmt.Errorf("not support fs PutStream")
	}
	fs.Logf(nil, "[remote] PutStream %s", src.Remote())
	return putStreamer.PutStream(ctx, in, src, options...)
}

/********************************************** fs.Mover *******************************************************/

func (f *Hvfs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	mover, ok := f.localFs.(fs.Mover)
	if !ok {
		return nil, fmt.Errorf("not support fs Move")
	}
	fs.Logf(nil, "[remote] Move %s => %s", src.Remote(), remote)
	return mover.Move(ctx, src, remote)
}

/********************************************** fs.DirMover *******************************************************/

func (f *Hvfs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	dirMover, ok := f.localFs.(fs.DirMover)
	if !ok {
		return fmt.Errorf("not support fs DirMove")
	}
	fs.Logf(nil, "[remote] DirMove %s => %s", src.Name(), dstRemote)
	return dirMover.DirMove(ctx, src, srcRemote, dstRemote)
}

/********************************************** fs.OpenWriterAter *******************************************************/

func (f *Hvfs) OpenWriterAt(ctx context.Context, remote string, size int64) (fs.WriterAtCloser, error) {
	openWriterAter, ok := f.localFs.(fs.OpenWriterAter)
	if !ok {
		return nil, fmt.Errorf("not support fs OpenWriterAt")
	}

	return openWriterAter.OpenWriterAt(ctx, remote, size)
}

/*********************************************** fs.DirSetModTimer ******************************************************/

func (f *Hvfs) DirSetModTime(ctx context.Context, dir string, modTime time.Time) error {
	dirSetModTimer, ok := f.localFs.(fs.DirSetModTimer)
	if !ok {
		return fmt.Errorf("not support fs DirSetModTime")
	}

	return dirSetModTimer.DirSetModTime(ctx, dir, modTime)
}

/********************************************** fs.MkdirMetadataer *******************************************************/

func (f *Hvfs) MkdirMetadata(ctx context.Context, dir string, metadata fs.Metadata) (fs.Directory, error) {
	mdkirMetaDataer, ok := f.localFs.(fs.MkdirMetadataer)
	if !ok {
		return nil, fmt.Errorf("not support fs MkdirMetadata")
	}

	return mdkirMetaDataer.MkdirMetadata(ctx, dir, metadata)
}

/*********************************************** fs.Commander ******************************************************/

func (f *Hvfs) Command(ctx context.Context, name string, arg []string, opt map[string]string) (interface{}, error) {
	commander, ok := f.localFs.(fs.Commander)
	if !ok {
		return nil, fmt.Errorf("not support fs Command")
	}

	return commander.Command(ctx, name, arg, opt)
}

/*********************************************** fs.About ******************************************************/

func (f *Hvfs) About(ctx context.Context) (*fs.Usage, error) {
	var usage fs.Usage
	fs.Logf(nil, "[remote] get usage")
	return &usage, nil
}

/************************************************** Object ***************************************************/

// Object represents a local filesystem object
type Object struct {
	fs          *Hvfs // The Fs this object is part of
	localObject *local.Object
}

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.localObject.String()
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.localObject.Remote()
}

// ModTime returns the modification time of the object
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.localObject.ModTime(ctx)
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.localObject.Size()
}

func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	return o.localObject.Hash(ctx, ty)
}

func (o *Object) Storable() bool {
	return o.localObject.Storable()
}

func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return o.localObject.SetModTime(ctx, t)
}

func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.Logf(nil, "[remote] Open %s", o.Remote())
	return o.localObject.Open(ctx, options...)
}

func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	fs.Logf(nil, "[remote] Update %s", src.Remote())
	return o.localObject.Update(ctx, in, src, options...)
}

func (o *Object) Remove(ctx context.Context) error {
	fs.Logf(nil, "[remote] Remove %s", o.Remote())
	return o.localObject.Remove(ctx)
}

/*****************************************************************************************************/

func (o *Object) Metadata(ctx context.Context) (metadata fs.Metadata, err error) {
	return o.localObject.Metadata(ctx)
}

/*****************************************************************************************************/

func (o *Object) SetMetadata(ctx context.Context, metadata fs.Metadata) error {
	return o.localObject.SetMetadata(ctx, metadata)
}

/************************************************* Directory ****************************************************/

// Directory represents a local filesystem directory
type Directory struct {
	Object
}

func (d *Directory) Items() int64 {
	return -1
}

func (d *Directory) ID() string {
	return ""
}

/*************************************************** check **************************************************/

var (
	_ fs.Fs              = &Hvfs{}
	_ fs.PutStreamer     = &Hvfs{}
	_ fs.Mover           = &Hvfs{}
	_ fs.DirMover        = &Hvfs{}
	_ fs.Commander       = &Hvfs{}
	_ fs.OpenWriterAter  = &Hvfs{}
	_ fs.DirSetModTimer  = &Hvfs{}
	_ fs.MkdirMetadataer = &Hvfs{}
	_ fs.Object          = &Object{}
	_ fs.Metadataer      = &Object{}
	_ fs.SetMetadataer   = &Object{}
	_ fs.Directory       = &Directory{}
	_ fs.SetModTimer     = &Directory{}
	_ fs.SetMetadataer   = &Directory{}
)
