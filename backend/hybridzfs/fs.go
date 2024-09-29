package hybridzfs

import (
	"context"
	"runtime"
	"time"

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

type Hvfs struct {
	localFs  fs.Fs // is same as cache vfs fs
	sqlFs    fs.Fs
	remoteFs fs.Fs
}

func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	f, err := local.NewFs(ctx, name, root, m)
	if err != nil {
		return nil, err
	}

	fs.Logf(nil, "name=%s, root=%s", name, root)

	return f, nil
}
