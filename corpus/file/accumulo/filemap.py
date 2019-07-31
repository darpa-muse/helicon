import os
import re
import json
import time
import collections

import numpy as np


BASE = '/data'
#ONE = '0to7'
#TWO = '8tof'
ONE = 'corpus_0to7'
TWO = 'corpus_8tof'

UUID_PATTERN = '^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$'
MD5_PATTERN = '^[0-9a-f]{32}$'
SIZE = 'file_sizes.txt'
HASH = 'md5deep.txt'

def build_path(uuid):
    if len(uuid) != 36:
        raise ValueError('length of {} is not 36'.format(uuid))
    uuid = uuid.lower()
    if re.match(UUID_PATTERN, uuid) is None:
        raise ValueError('{} not a valid UUID'.format(uuid))
    
    dirs = [BASE]
    if uuid[0] in '01234567':
        dirs.append(ONE)
    else:
        dirs.append(TWO)

    dirs.extend(uuid[:8])
    dirs.append(uuid)
    return os.path.join(*dirs)


def get_uuids(path='data/uuids.json'):
    with open(path) as f:
        return json.load(f)


def mine_uuids(path1='/data/0to7', path2='/data/8tof', limit=None):
    uuids = []
    roots = []
    roots.extend([os.path.join(path1, x) for x in '01234567'])
    roots.extend([os.path.join(path2, x) for x in '89abcdef'])
    start = time.time()
    for r in roots:
        for root, dirs, files in os.walk(r):
            base = os.path.basename(root)
            if re.match(UUID_PATTERN, base):
                uuids.append(base)
                if not np.log2(len(uuids)) % 1:
                    delta = int(time.time() - start)
                    print '{} uuids, {} seconds'.format(len(uuids), delta)
                if limit and len(uuids) >= limit:
                    uuids.sort()
                    return uuids
                dirs[:] = []
    uuids.sort()
    return uuids
                

def sizes(uuid):
    path = build_path(uuid)
    with open(os.path.join(path, SIZE)) as f:
        lines = [x.strip() for x in f]
        if lines[0]:
            raise ValueError('{} has nonzero first line'.format(uuid))
        else:
            lines = lines[1:]
    output = []
    for line in lines:
        tokens = line.split(',')
        if len(tokens) < 1:
            raise ValueError('no "," separator in line')
        output.append((os.path.join(uuid, ','.join(tokens[1:])), int(tokens[0])))
    output.sort()
    return output


def hashes(uuid):
    path = build_path(uuid)
    with open(os.path.join(path, HASH)) as f:
        lines = [x.strip().split() for x in f]
    if any(len(x) != 2 for x in lines):
        raise ValueError('each hash must have two lines')
    output = []
    for md5, filepath in lines:
        md5 = md5.lower()
        if len(md5) != 32:
            raise ValueError('{} not length 32 md5'.format(md5))
        if not re.match(MD5_PATTERN, md5):
            raise ValueError('{} not a valid md5'.format(md5))
        if not filepath.startswith('/md5tmp/'):
            raise ValueError('path does not start with /md5tmp/')
        filepath = filepath[len('/md5tmp/'):]
        if not filepath.startswith(uuid + '/'):
            raise ValueError('path does not start with /md5tmp/<uuid>/')
        output.append((filepath, md5))
    output.sort()
    return output


EXTS = set([
    'asm',
    'c',
    'c++',
    'cc',
    'cpp',
    'cs'
    'cxx',
    'h',
    'hh',
    'hxx',
    'java',
    'js',
    'm',
    'mm',
    'php',
    'pl',
    'py',
    'r',
    'rb',
    'scala',
    'sh',
    'vb',
])


def path_filter(path):
    """
    Return True if path needs to be kept (not source code)
    """
    basename = os.path.basename(path)
    if '.' not in basename:
        return False
    ext = basename.split('.')[-1]
    return ext.lower() in EXTS


def get(uuid, filter_git=True):
    """
    Return paths, sizes, hashes
    """
    paths, s = zip(*sizes(uuid))
    paths2, h = zip(*hashes(uuid))
    if paths != paths2:
        raise ValueError('paths != paths2')

    # code always has 'content' (svn) or 'latest' (git)    
    if filter_git:
        zipped = []
        for path, size, hash_ in zip(paths, s, h):
            if path_filter(path):
                zipped.append((path, size, hash_))
        paths, s, h = zip(*zipped)
    return paths, s, h


class Mapper(object):
    def __init__(self):
        self.uuids = set()
        self.sizes = {}  # map md5 hash to file size
        self.paths = {}  # map md5 hash to [file path]
        self.counts = collections.Counter()  # map md5 hash to count 

    @classmethod
    def from_other(cls, other):
        mapper = cls()
        mapper.uuids = other.uuids
        mapper.sizes = other.sizes
        mapper.paths = other.paths
        mapper.counts = other.counts
        return mapper

    def add(self, uuid):
        if uuid in self.uuids:
            raise ValueError('{} already added'.format(uuid))
        try:
            path_list, size_list, hash_list = get(uuid)

            for p, s, h in zip(path_list, size_list, hash_list):
                if h in self.sizes and self.sizes[h] != s:
                    raise ValueError('{} has inconsistent sizes'.format(h))

            for p, s, h in zip(path_list, size_list, hash_list):
                if h not in self.sizes:
                    self.sizes[h] = s
                    self.paths[h] = []
                self.paths[h].append(p)
                self.counts[h] += 1

            self.uuids.add(uuid)
        except Exception as e:
            print 'Exception on {}: {}'.format(uuid, e)

    def content_size(self, dedup=True):
        """
        Return size of content
        """

        total = 0
        for h, s in self.sizes.iteritems():
            if dedup:
                total += s
            else:
                total += self.counts[h] * s
        return total

    def meta_size(self):
        """
        Size of hashes, paths, sizes, and counts in bytes
        """
        total = (32 + 8 + 8) * len(self.sizes)  # hashes, sizes, counts
        for paths in self.paths.itervalues():
            total += sum(len(x) for x in paths)
        return total

    def save(self, filepath):
        with open(filepath, 'w') as f:
            json.dump({
                'uuids': list(self.uuids),
                'sizes': self.sizes,
                'paths': self.paths,
                'counts': self.counts,
            }, f)

    def load(self, filepath):
        with open(filepath) as f:
            x = json.load(f)
            self.uuids = set(x['uuids'])
            self.sizes = x['sizes']
            self.paths = x['paths']
            self.counts = collections.Counter(x['counts'])
        return self
