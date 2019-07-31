"""
Move UUID directories from source directory to target octet directory
"""

import sys
import os
import shutil
import itertools
import re

COPY = False  # whether to copy instead of move

if len(sys.argv) != 3:
    print 'Usage: python project_map <source_dir> <target_dir>'
    sys.exit()

source_dir = sys.argv[1]
target_dir = sys.argv[2]

if not os.path.isdir(target_dir):
    raise IOError('target_dir {} is not a directory'.format(target_dir))

for i in itertools.product(*(['0123456789abcdef'] * 2)):
    sub_dir = os.path.join(target_dir, ''.join(i))
    if not os.path.isdir(sub_dir):
        raise IOError('sub_dir {} is not a directory'.format(sub_dir))

if not os.path.isdir(source_dir):
    raise IOError('source_dir {} is not a directory'.format(source_dir))

print 'traversing source_dir'

UUID_PATTERN = '^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$'
def is_uuid(uuid):
    if len(uuid) != 36:
        return False
    if uuid != uuid.lower():
        return False
    if re.match(UUID_PATTERN, uuid) is None:
        return False
    return True

SET = set(''.join(x) for x in itertools.product(*(['0123456789abcdef'] * 2)))

def move(source_subdir):
    """Uses above target_dir and assumed UUID"""
    if not os.path.isdir(source_subdir):
        raise IOError('source_subdir {} is not a directory'.format(source_subdir))
    uuid = os.path.basename(source_subdir)
    assert is_uuid(uuid), 'move needs UUID input'
    target_subdir = os.path.join(target_dir, uuid[:2], uuid)
    if os.path.exists(target_subdir):
        raise ValueError('target {} already exists'.format(target_subdir))
    
    print uuid
    if COPY:
        shutil.copytree(source_subdir, target_subdir)
    else:
        shutil.move(source_subdir, target_subdir)


if is_uuid(source_dir):
    print 'moving {}'.format(source_dir)
    move(source_dir)
    sys.exit()


for root, dirs, files in os.walk(source_dir):
    uuids = [d for d in dirs if is_uuid(d)]
    if uuids:
        # if uuid, copy to target and stop recursing
        dirs[:] = []
        for uuid in uuids:
            move(os.path.join(root, uuid))
    else:
        # if not, filter folders for 0-f and recurse
        dirs[:] = [d for d in dirs if d in '0123456789abcdef']
        dirs.sort()
