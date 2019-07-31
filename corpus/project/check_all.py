"""
Check all of the directories in find. Add successes to success.

Remove misses and rerun on those
"""

import os

from muse import tars, constant
from muse.constant import TOTAL, SUCCESS, FAIL


def main():
    remaining = constant.get_remaining()

    with open(SUCCESS, 'a') as f_success, open(FAIL, 'w') as f_fail:
        for i, uuid_path in enumerate(remaining):
            print '{}'.format(i).rjust(7), uuid_path, 
            if tars.check_dir(uuid_path, ignore_missing_pairs=True) is None:
                f_success.write(uuid_path + '\n')
                print 'success'
            else:
                f_fail.write(uuid_path + '\n')
                print 'failure'


if __name__ == '__main__':
    main()
