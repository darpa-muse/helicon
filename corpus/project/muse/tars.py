"""
Functions for checking uuid directories
"""

import os
import shutil
import subprocess

from muse import util


RESERVED = [
    'meta_hashdeep.txt',
    'code_hashdeep.txt',
    'extra_hashdeep.txt',
    'main_hashdeep.txt',
]
TARS = [
    '_metadata.tgz',
    '_code.tgz',
    '.tgz',
]


def check_dirs(basedir, **kwargs):
    """
    Check all directories in directory
    """
    flag = False
    for d in util.walk_uuids(basedir):
        result = check_dir(d, **kwargs)
        if result is not None:
            flag = True
            print d, result
    return not flag


def check_dir(uuid_directory, ignore_main=False, ignore_missing_pairs=False):
    """
    Check uuid directory for the correct files.

    Returns None or two lists: missing, extra

    ignore_main - whether to check for main_hashdeep.txt
    ignore_missing_pairs - whether to ignore missing pairs 
    """

    uuid = os.path.basename(uuid_directory)
    if not util.is_uuid(uuid):
        raise ValueError('{} is not a uuid'.format(uuid_directory))
    if not os.path.isdir(uuid_directory):
        raise ValueError('{} is not a directory'.format(uuid_directory))

    missing, extra = [], []
    reserved = RESERVED + [uuid + ending for ending in TARS]
    if ignore_main:
        reserved.remove('main_hashdeep.txt')

    for filename in reserved:
        if not os.path.isfile(os.path.join(uuid_directory, filename)):
            missing.append(filename)
   
    for filename in os.listdir(uuid_directory):
        if filename not in reserved:
            extra.append(filename)

    if ignore_missing_pairs:
        ignored = []
        for r, t in zip(RESERVED, TARS):
            if r in missing and uuid + t in missing:
                missing.remove(r)
                missing.remove(uuid + t)

    if missing or extra:
        return missing, extra
    return None


def remove_empty_dirs(basedir):
    """
    Remove empty subdirectories of basedir, without recursion
    """
    for root, directories, files in os.walk(basedir):
        for d in directories:
            path = os.path.join(root, d)
            if not os.listdir(path):  # empty directory
                print path
                os.rmdir(path)
        break


def remove_all_empty_dirs(basedir):
    for d in util.walk_uuids(basedir):
        remove_empty_dirs(d)


def move_builds(source_base, target_base):
    """
    Move all the build directories from source to target, recursive
    """
    if source_base.endswith('/'):
        source_base = source_base[:-1]
    if target_base.endswith('/'):
        target_base = target_base[:-1]

    if not source_base:
        raise ValueError('cannot have empty or root source_base')
    if source_base == target_base:
        raise ValueError('source_base and target_base are identical')
    #for x in (source_base, target_base):
    #    if not util.is_octet_dir(x):
    #        raise ValueError('{} is not an octet directory'.format(x))

    for source in util.walk_uuids(source_base):
        # remove source_base
        source = os.path.join(source, 'buildResults')
        target = target_base + source[len(source_base):]
        if os.path.isdir(source):
            print 'moving {} to {}'.format(source, target)
            shutil.move(source, target)
        

def remove_filesizes_recursive(source_base):
    """
    Remove all file_sizes.txt files
    """
    for dirpath in util.walk_uuids(source_base):
        remove_filesizes(dirpath)


def remove_filesizes(dirpath):
    path = os.path.join(dirpath, 'file_sizes.txt')
    if os.path.isfile(path):
        print 'Removing {}'.format(path)
        os.remove(path)
    


def remove_links(dir_path):
    '''
    Remove all symlinks in a target directory

    @param dir_path: target directory to remove links from
    '''
    if os.path.islink(dir_path):
        raise ValueError('root target directory cannot be a link')

    # find external links
    links = []
    for root, dirs, files in os.walk(dir_path):
        for d in dirs:
            path = os.path.join(root, d)
            if os.path.islink(path):
                links.append(path)

    # remove external links
    for path in links:
        os.remove(path)


def extract_project(project_dir, tar):
    '''
    @param project_dir: the path to the PROJECT, e.g. 'stuff/stuff/UUID/''
    @param tar: the name of the tar to exract
    '''
    tgz_path = project_dir + tar
    tmp_dir = project_dir + 'tmp'

    try:
        command = 'tar -zxf {} -C {}'.format(tgz_path, tmp_dir).split()
        subprocess.call(command)
    except subprocess.CalledProcessError as e:
        raise ValueError('{} not extracted correctly: {}'.format(
            project_dir, e))

    remove_links(tmp_dir)

    try:
        command = 'du -s {}'.format(tmp_dir).split()
        string = subprocess.check_output(command)
    except subprocess.CalledProcessError as e:
        raise ValueError('{} not extracted correctly: {}'.format(
            project_dir, e))

    return string


#def hash_dir(dir_path):
#    """
#    Hash the entire extracted directory using hashdeep
#    """
#    try:
#        command = 'hashdeep -c md5 -s -of -r {}'.format(dir_path).split()
#        string = subprocess.check_output(command)
#    except subprocess.CalledProcessError as e:
#        raise ValueError('{} not hashed correctly {}'.format(dir_path, e))
#
#    return string


def hash_dir_to_file(dir_path, target='main_hashdeep.txt'):
    """
    Produce hash file in target directory
    """
    filepath = os.path.join(dir_path, target)
    if os.path.exists(filepath):
        print '{} already exists. Skipping'.format(filepath)
        return

    string = hash_dir(dir_path)
    with open(filepath) as f:
        f.write(string)




def move_hashes(content, tmp_dir, target_dir):
    """
    Move the paths in a hashdeep file away from tmp directories
    """
    pass


def hash_to_file(dir_path, target='main_hashdeep.txt'):
    """
    Produce hash file in target directory
    """

    if os.path.islink(dir_path):
        raise ValueError('directory cannot be a link')

    filepath = os.path.join(dir_path, target)
    if os.path.exists(filepath):
        print '{} already exists. Skipping'.format(filepath)

    with open(filepath, 'w') as f:
        f.write(hash_dir(dir_path))


#def hash_dir(dir_path):
#    """
#    Produce hash file content, removing unnecessary lines
#    """
#    try:
#        command = 'hashdeep -c md5 -s -o f -r {}'.format(dir_path).split()
#        hashdeep = subprocess.check_output(command)
#        hash_lines = hashdeep.split('\n')
#        hash_lines = [hash_lines[1]] + hash_lines[5:]  # remove 0, 2, 3, 4
#        return '\n'.join(hash_lines)
#    except subprocess.CalledProcessError as e:
#        raise ValueError('{} not hashed correctly {}'.format(dir_path, e))


def hash_dir(dir_path):
    """
    Produce hash file content, removing unnecessary lines
    """
    if os.path.islink(dir_path):
        raise ValueError('directory cannot be a link')

    try:
        command = 'hashdeep -c md5 -s -o f -r {}'.format(dir_path).split()
        hashdeep = subprocess.check_output(command)
        if hashdeep == '': # no files
            return '%%%% size,md5,filename\n'

        hash_lines = hashdeep.split('\n')
        hash_lines = [hash_lines[1]] + hash_lines[5:]  # remove 0, 2, 3, 4
        return '\n'.join(hash_lines)
    except subprocess.CalledProcessError as e:
        raise ValueError('{} not hashed correctly {}'.format(dir_path, e))

def read_hashdeep(filepath):
    """
    Read the hashdeep file related to tarball

    Return list of (size, md5, filepath) tuples
    """
    lines = []
    with open(filepath) as f:
        header = f.next()
        if header != '%%%% size,md5,filename\n':
            raise ValueError('{} incorrect header {}'.format(filepath, header))
        for i, line in enumerate(f, 1):
            line = line.rstrip('\n')
            if line:
                size, md5, path = line.split(',', 2)
                if not is_md5(md5):
                    raise ValueError('{}:{} {} is not an md5'.format(
                        filepath, i, md5))
                lines.append((int(size), md5, path))
    return lines


