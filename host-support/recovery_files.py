"""Private Linux file helpers. No credential-specific parsing or logging."""
import contextlib
import json
import os
from pathlib import Path
import stat
import uuid


def metadata(st, directory=False, private=True):
    valid = stat.S_ISDIR(st.st_mode) if directory else stat.S_ISREG(st.st_mode)
    if (not valid or st.st_uid != os.getuid() or
            (private and st.st_mode & 0o077) or
            (not directory and st.st_nlink != 1)):
        raise ValueError("unsafe_file_metadata")


@contextlib.contextmanager
def directory(path, private=True):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError("absolute_clean_path_required")
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in path.parts[1:]:
            nxt = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW |
                          os.O_CLOEXEC, dir_fd=fd)
            os.close(fd)
            fd = nxt
        metadata(os.fstat(fd), directory=True, private=private)
        yield fd
    finally:
        os.close(fd)


def read_bytes(path, limit=16384, private=True):
    path = Path(path)
    with directory(path.parent, private=private) as parent:
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK |
                     os.O_CLOEXEC, dir_fd=parent)
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            metadata(before, private=private)
            if before.st_size > limit:
                raise ValueError("oversized_file")
            data = stream.read(limit + 1)
            after = os.fstat(stream.fileno())
            if (len(data) > limit or (before.st_size, before.st_mtime_ns,
                    before.st_ctime_ns) != (after.st_size, after.st_mtime_ns,
                                           after.st_ctime_ns)):
                raise ValueError("file_changed_or_oversized")
            return data


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result


def read_json(path):
    return json.loads(read_bytes(path), object_pairs_hook=unique_object)


def write_bytes(path, data, replace=False):
    path = Path(path)
    with directory(path.parent) as parent:
        temporary = '.recovery-' + uuid.uuid4().hex
        fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                     os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=parent)
        try:
            with os.fdopen(fd, 'wb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            if replace:
                try:
                    metadata(os.stat(path.name, dir_fd=parent, follow_symlinks=False))
                except FileNotFoundError:
                    pass
                os.replace(temporary, path.name, src_dir_fd=parent, dst_dir_fd=parent)
            else:
                # Atomic no-replace install, including against an existing symlink.
                os.link(temporary, path.name, src_dir_fd=parent,
                        dst_dir_fd=parent, follow_symlinks=False)
                os.unlink(temporary, dir_fd=parent)
            os.fsync(parent)
        finally:
            try:
                os.unlink(temporary, dir_fd=parent)
            except FileNotFoundError:
                pass


def write_json(path, value):
    write_bytes(path, (json.dumps(value, indent=2) + '\n').encode(), replace=True)


def make_private_directory(path):
    path = Path(path)
    with directory(path.parent) as parent:
        try:
            os.mkdir(path.name, 0o700, dir_fd=parent)
            os.fsync(parent)
        except FileExistsError:
            pass
    with directory(path):
        pass


@contextlib.contextmanager
def private_lock(path):
    import fcntl
    path = Path(path)
    with directory(path.parent) as parent:
        fd = os.open(path.name, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW |
                     os.O_NONBLOCK | os.O_CLOEXEC, 0o600, dir_fd=parent)
        try:
            metadata(os.fstat(fd))
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield fd
        finally:
            os.close(fd)
