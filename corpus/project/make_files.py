"""
Make the <00-ff> octect file structure in target directory
    octet_size - number of octets (00-ff is 2 octets)
    dir_path - target directory
"""

import sys
import os
import itertools

if len(sys.argv) != 3:
    print 'Usage: python make_files <octet_size> <dir_path>'
    sys.exit()

octet_size = int(sys.argv[1])
if octet_size < 1 or octet_size > 4:
    raise ValueError('octet must be in range {1, 2, 3, 4}')

root = sys.argv[2]
print 'making {} directory'.format(root)
if not os.path.exists(root):
    os.mkdir(root)

print 'making {} subdirectories'.format(2**(4*octet_size))
for i in itertools.product(*(['0123456789abcdef'] * octet_size)):
    subdir = ''.join(i)
    os.mkdir(os.path.join(root, subdir))
