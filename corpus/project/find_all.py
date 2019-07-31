"""
Find all uuid directories
"""

import os

from muse import util, constant


FIND_FILE = 'log/uuid_dirs.txt'


def main(root_dirs):
    if os.path.exists(constant.TOTAL):
        print 'File already exists. Must delete manually'
        return
    root_dirs.sort()
    with open(constant.TOTAL, 'w') as f:
        for root_dir in root_dirs:
            print 'Starting {}'.format(root_dir)
            for i, uuid_dir in enumerate(util.walk_uuids(root_dir)):
                if not (i % 1000):
                    print uuid_dir
                f.write(uuid_dir + '\n')


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print 'Usage: python find_all.py <root_dir1> [<root_dir2> ...]'
        sys.exit()
    main(sys.argv[1:])
