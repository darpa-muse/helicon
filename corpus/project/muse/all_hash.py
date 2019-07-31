"""
Generate full corpus hashes
"""

import os

from muse import constant, util

HASH = 'main_hashdeep.txt'
HEADER = '%%%% size,md5,filename\n'


def do_all(uuid_dirs):
    succeeded, failed, errors = pull_all(uuid_dirs)
    if failed or errors:
        print 'FAILURE in pull_all'
        return succeeded, failed, errors
    
    print 'combining'
    return combine(succeeded)


def pull_all(uuid_dirs):
    succeeded, failed, errors = [], [], []
    for i, uuid_dir in enumerate(uuid_dirs):
        try:
            with open(os.path.join(uuid_dir, HASH)) as f:
                d = f.read()
            succeeded.append((uuid_dir, d))
        except KeyboardInterrupt:
            print 'Breaking'
            break
        except Exception as e:
            print uuid_dir, e
            failed.append(uuid_dir)
            errors.append(str(e))
        
        if (i + 1) % 100 == 0:
            print i + 1, uuid_dir
    return succeeded, failed, errors


def combine(succeeded):
    uuid_dirs, data = zip(*succeeded)
    final = [HEADER[:-1]]  # header without '\n'
    for i, d in enumerate(data):
        uuid_dir = uuid_dirs[i]
        final.extend(filter_file(uuid_dir, d))
    final.append('')
    return '\n'.join(final)


def filter_file(uuid_dir, d):
    filtered = []
    if d.startswith(HEADER):
        uuid = uuid_dir[-36:]
        d = d[len(HEADER):]
        lines = d.split('\n')
        for line in lines:
            if keep_line(line, uuid_dir, uuid):
                filtered.append(line)
    else:
        print '{} does not start with HEADER!'.format(uuid_dir)
    return filtered


def keep_line(line, uuid_dir, uuid):
    """
    Return whether line should be kept
    """
    tokens = line.split(',', 2)
    if len(tokens) != 3:
        return False
    size, md5, path = tokens
    try:
        int(size)
    except ValueError:
        return False
    if not util.is_md5(md5):
        return False
    if not path.startswith(uuid_dir + '/'):
        return False
    # remove ending
    trailing = path[len(uuid_dir) + 1:]
    if trailing in ('code_hashdeep.txt', 'meta_hashdeep.txt', 'extra_hashdeep.txt'):
        return True
    if not trailing.startswith(uuid):
        return False
    trailing = trailing[36:]  # remove uuid
    if trailing in ('_code.tgz', '_metadata.tgz', '.tgz'):
        return True
    return False


def remove_main_hashes(uuid_dirs, dryrun=True):
    for uuid_dir in uuid_dirs:
        path = os.path.join(uuid_dir, HASH)
        print 'deleting {}'.format(path)
        if not dryrun:
            os.remove(path)


def remove_bash(bash_filepath, uuid_dirs):
    """
    Generate bash script to remove all main_hashdeep files
    """
    with open(bash_filepath, 'w') as f:
        for uuid_dir in uuid_dirs:
            path = os.path.join(uuid_dir, HASH)
            f.write('rm {}\n'.format(path))

