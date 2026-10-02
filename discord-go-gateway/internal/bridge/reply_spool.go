package bridge

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// PruneReplySpool removes only unreferenced or fully-sent immutable blobs older
// than seven days. Pending, failed, sending, uncertain, and cancelled-unsent
// output remains protected. The hard quota is fail-closed even if all files are
// protected, rather than silently dropping a queued/uncertain attachment.
func (s *Store) PruneReplySpool() (int, error) {
	v, e := s.call(func(db *storeConn) (any, error) {
		return transact(db, func(db *storeConn) (any, error) {
			keep := map[string]bool{}
			rows, e := db.Query(`SELECT o.payload FROM reply_outputs o WHERE EXISTS(SELECT 1 FROM chunks c WHERE c.reply_id=o.reply_id AND c.idx=0 AND c.state!='sent') UNION ALL SELECT o.payload FROM reply_followup_outputs o JOIN chunks c ON c.reply_id=o.reply_id AND c.idx=o.idx WHERE c.state!='sent'`)
			if e != nil {
				return nil, e
			}
			for rows.Next() {
				var raw string
				if e = rows.Scan(&raw); e != nil {
					rows.Close()
					return nil, e
				}
				out, e := decodeReplyOutput(ReplyOutputSnapshot(raw))
				if e != nil {
					rows.Close()
					return nil, e
				}
				for _, f := range out.Attachments {
					keep[f.SHA256+".blob"] = true
				}
			}
			e = rows.Err()
			rows.Close()
			if e != nil {
				return nil, e
			}
			if e := ensureReplyStateRoot(s.replyStateDir); e != nil {
				return nil, e
			}
			spool, e := ensureReplyDirectory(s.replyStateDir, "reply-spool")
			if e != nil {
				return nil, e
			}
			defer spool.Close()
			lockFD, e := unix.Openat(int(spool.Fd()), ".lock", unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
			if e != nil {
				return nil, errors.New("reply_spool_unavailable")
			}
			lock := os.NewFile(uintptr(lockFD), "reply-spool-lock")
			defer lock.Close()
			li, e := lock.Stat()
			if e != nil || !replyOwned(li) || !li.Mode().IsRegular() || li.Mode().Perm()&0077 != 0 {
				return nil, errors.New("reply_spool_unavailable")
			}
			if unix.Flock(lockFD, unix.LOCK_EX) != nil {
				return nil, errors.New("reply_spool_unavailable")
			}
			defer unix.Flock(lockFD, unix.LOCK_UN)
			entries, e := spool.ReadDir(-1)
			if e != nil {
				return nil, e
			}
			removed := 0
			before := time.Now().Add(-7 * 24 * time.Hour)
			for _, entry := range entries {
				name := entry.Name()
				digest, ok := strings.CutSuffix(name, ".blob")
				if !ok || !replyDigestPattern.MatchString(digest) || keep[name] {
					continue
				}
				info, e := entry.Info()
				st, valid := beforeSys(info)
				if e != nil || !valid || st.Nlink != 1 || !replyOwned(info) || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || !info.ModTime().Before(before) {
					continue
				}
				if e = unix.Unlinkat(int(spool.Fd()), name, 0); e != nil {
					return nil, errors.New("reply_spool_cleanup_failed")
				}
				removed++
			}
			if removed > 0 {
				if e = spool.Sync(); e != nil {
					return nil, errors.New("reply_spool_cleanup_failed")
				}
			}
			return removed, nil
		})
	})
	if e != nil {
		return 0, e
	}
	return v.(int), nil
}

// ReadReplyManifestFile does not follow symlinks or wait on a FIFO/device. The
// manifest itself is local JSON only and must still pass the strict parser;
// attachments can name only an outbox basename, never this file's directory.
func ReadReplyManifestFile(path string) ([]byte, error) {
	absolute, e := filepath.Abs(path)
	if e != nil {
		return nil, errors.New("reply_manifest_file_unavailable")
	}
	// Manifest parents need not be private, but every ancestor is opened without
	// following symlinks. The file must be owned and not group/world writable.
	fd, e := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, errors.New("reply_manifest_file_unavailable")
	}
	for _, part := range strings.Split(strings.TrimPrefix(filepath.Dir(absolute), "/"), "/") {
		if part == "" {
			continue
		}
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if err != nil {
			return nil, errors.New("reply_manifest_file_unavailable")
		}
		fd = next
	}
	dir := os.NewFile(uintptr(fd), "manifest-parent")
	defer dir.Close()
	data, e := readReplyFile(dir, filepath.Base(absolute), MaxReplyManifestBytes, false)
	if e != nil {
		return nil, errors.New("reply_manifest_file_unavailable")
	}
	return data, nil
}
