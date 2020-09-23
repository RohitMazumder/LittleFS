#!/usr/bin/env python

""" LittleFS.py: Data-deplication based filesystem written using FUSE API"""

__author__ = "Rohit Mazumder"

import os
import sys
import errno
import hashlib
import sqlite3
import math

from fuse import FUSE, FuseOSError, Operations

# Constants

DEFAULT_DATABASE_FILE = '~/.littleFS-datastore.db'
HASH_SIZE = 64
DEFAULT_BLOCK_SIZE = 4096


class LittleFS(Operations):

    def __init__(self, args):
        self._validate_database_file(args.database_file)
        self.database_file = args.database_file
        self.root = os.path.realpath(args.root)

        self._setup_database_connection()
        self._create_table(args)

        self._initialize_options()

    def _validate_database_file(self, database_file):
        '''
        In case the database_file exists, but is not readable or writable.
        '''
        fullpath = os.path.expanduser(database_file)
        if os.access(fullpath, os.F_OK) and (not os.access(fullpath, os.R_OK)
                                             or not os.access(fullpath, os.W_OK)):
            raise CorruptDatabaseException(
                'The database file %s is not readable/writeable' % database_file)

    def _setup_database_connection(self):
        self.conn = sqlite3.connect(os.path.expanduser(self.database_file))

    def _create_table(self, args):
        self.conn.executescript(""" 
            CREATE TABLE IF NOT EXISTS hashes (
            hash VARCHAR(64) PRIMARY KEY, 
            block VARCHAR
            );

            CREATE TABLE IF NOT EXISTS options (
            option TEXT PRIMARY KEY,
            value TEXT NOT NULL);

            INSERT OR IGNORE INTO options (option, value) VALUES ('block_size', %i)
            """ % (args.block_size))

    def _initialize_options(self):
        cur = self.conn.cursor()
        cur.execute("""SELECT * FROM options""")
        rows = cur.fetchall()

        for option, value in rows:
            if option == 'block_size':
                self.block_size = int(value)

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    def access(self, path, mode):
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        full_path = self._full_path(path)
        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                        'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def readdir(self, path, fh):
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r

    def readlink(self, path):
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
                                                         'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
                                                         'f_frsize', 'f_namemax'))

    def unlink(self, path):
        return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        return os.symlink(name, self._full_path(target))

    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        return os.link(self._full_path(target), self._full_path(name))

    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    def open(self, path, flags):
        full_path = self._full_path(path)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        contents = os.read(fh, length)
        return contents

    def write(self, path, buf, offset, fh):
        new_buf = ''
        for block_num in range(int(math.ceil(len(buf) / self.block_size))):
            curr_block = self._read_block(
                buf, block_num * self.block_size, self.block_size)
            block_hash = hashlib.sha256(curr_block).hexdigest()
            new_buf += block_hash
            cursor = self.conn.cursor()
            cursor.execute(
                """SELECT * FROM hashes WHERE hash = ?""", (block_hash,))
            rows = cursor.fetchall()
            if (len(rows) == 0):
                cursor.execute(
                    'INSERT INTO hashes (hash, block) VALUES (?, ?)', (block_hash, curr_block))
            self.conn.commit()
            cursor.close()

        offset = (offset * HASH_SIZE) // self.block_size
        os.lseek(fh, offset, os.SEEK_SET)
        os.write(fh, new_buf.encode())

        return len(buf)

    def _read_block(self, buf, offset, size):
        ''' An utility function to read a block of 'size' from buf starting from position defined by 'offset'
        In case, the buf has less than the number of characters required to form a block of 'size', 
        it will just return from the available characters'''

        if offset + size - 1 < len(buf):
            return buf[offset: offset + size - 1]
        return buf[offset:]

    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)


class CorruptDatabaseException(Exception):
    pass


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('root', help='root directory')
    parser.add_argument(
        'mount', help='directory to be mounted as the LittleFS')
    parser.add_argument('-bs', '--block_size', default=DEFAULT_BLOCK_SIZE, dest='block_size', type=int,
                        help='Block size for deduplication check. NOTE that this cannot changed once the filesystem is initialised for the first time')
    parser.add_argument('-db', '--database_file',
                        default=DEFAULT_DATABASE_FILE, dest='database_file')
    args = parser.parse_args()

    FUSE(LittleFS(args), args.mount, foreground=True, nothreads=True)
