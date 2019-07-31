"""
Script for a single tar directory
"""
import os
import subprocess
import shutil

from accumulo import filemap, util


TARGET_DIR = 'tmp'
MAX_SIZE = 2**27  # 128 MB


class Extractor(object):
    def __init__(self, uuid, max_size=MAX_SIZE, target_dir=TARGET_DIR,
                 empty_size=4096):
        util.check_uuid(uuid)
        self.uuid = uuid
        self.max_size = int(max_size)
        if not os.path.isdir(target_dir):
            raise ValueError('{} is not a directory'.format(target_dir))
        self.target_dir = target_dir
        self.empty_size = int(empty_size)
        if self.empty_size < 0:
            raise ValueError('empty_size cannot be < 0')

        path = filemap.build_path(uuid)
        ending = '{}_code.tgz'.format(uuid)
        self.source_path = os.path.join(path, ending)
        if not os.path.isfile(self.source_path):
            raise ValueError('{} is not a file'.format(self.source_path))

        check_tgz_size(self.source_path, empty_size=self.empty_size)
        self.dir_path = os.path.join(self.target_dir, uuid)

    def __enter__(self):
        os.mkdir(self.dir_path)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            command = 'chmod 777 -R {}'.format(self.dir_path)
            string = subprocess.check_output(command.split())
        except subprocess.CalledProcessError as e:
            shutil.rmtree(str(self.dir_path), ignore_errors=True)
            raise ValueError('chmod failed on {}: {}'.format(self.dir_path, e))
        shutil.rmtree(str(self.dir_path), ignore_errors=True)

    def extract(self):
        extract_project(self.source_path, self.dir_path)
        string = hash_dir(self.dir_path)
        self.tokens = format_hash(self.uuid, string)

    def get_tokens(self):
        tokens = []
        for size, md5, path in self.tokens:
            full_path = os.path.join(self.target_dir, path)
            tokens.append((size, md5, path, full_path))
        return tokens


def check_tgz_size(path, empty_size=4096, max_size=MAX_SIZE):
    path = str(path)
    if not os.path.isfile(path):
        raise ValueError('{} is not a file'.format(path))
    if not (path.endswith('.tgz') or path.endswith('.tar.gz')):
        raise ValueError('{} is not a gzip tar file'.format(path))
    if os.path.getsize(path) > max_size:
        size = os.path.getsize(path)
        raise ValueError('{} size {} > {} max'.format(path, size, max_size))

    try:
        string = subprocess.check_output('tar tvzf {}'.format(path).split())
    except subprocess.CalledProcessError as e:
        raise ValueError('{} tar call failed {}'.format(path, e))

    lines = [x for x in string.split('\n') if x]
    size = sum(max(int(x.split()[2]), empty_size) for x in lines)
    if size > max_size:
        raise ValueError('{} size {} > {} max'.format(path, size, max_size))
    return size


def extract_project(source_path, dir_path):
    """
    Extract gzip into tmp directory
    """
    try:
        command = 'tar -zxf {} -C {}'.format(source_path, dir_path).split()
        subprocess.call(command)
    except subprocess.CalledProcessError as e:
        raise ValueError('{} not extracted correctly {}'.format(
            source_path, e))

    remove_links(dir_path)


def hash_dir(dir_path):
    """
    Hash the entire extracted directory using hashdeep
    """
    try:
        command = 'hashdeep -c md5 -s -r {}'.format(dir_path)
        string = subprocess.check_output(command.split())
    except subprocess.CalledProcessError as e:
        raise ValueError('{} not hashed correctly {}'.format(dir_path, e))

    return string


def format_hash(uuid, string, path_filter=filemap.path_filter):
    """
    Turn hashdeep output into (size, md5, path) tuples.

    Trim paths to start at uuid
    """

    lines = []
    for x in string.split('\n'):
        if x and not x.startswith('%%%%') and not x.startswith('##'):
            tokens = x.split(',')
            if len(tokens) < 3:
                raise ValueError('len(tokens) must be at least 3')
            lines.append((int(tokens[0]), tokens[1], ','.join(tokens[2:])))

    if lines:
        sizes, md5s, paths = zip(*lines)
        index = paths[0].find(str(uuid))
        prefix = paths[0][:index]
        if any(p[:index] != prefix for p in paths):
            raise ValueError('differing prefixes for paths')
        paths = [p[index:] for p in paths]

        lines = zip(sizes, md5s, paths)

    if path_filter is not None:
        lines = filter(lambda x: path_filter(x[2]), lines)

    return lines


def remove_links(dir_path):
    """
    Remove symlinks that point external to the target directory

    Should be done prior to any computations on the untarred directory
    """
    if os.path.islink(dir_path):
        raise ValueError('root target directory cannot be a link')

    # find external links
    external_links = []
    full_dir = os.path.join(os.path.realpath(dir_path), '')  # add '/' to end
    for root, dirs, files in os.walk(dir_path):
        for d in dirs:
            path = os.path.join(root, d)
            if os.path.islink(path):
                target = os.readlink(path)
                full_target = os.path.realpath(target)
                if not full_target.startswith(full_dir):
                    external_links.append(path)

    # remove external links
    for path in external_links:
        os.remove(path)
