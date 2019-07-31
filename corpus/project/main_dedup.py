'''
Dedupe files that occur in both meta_hashdeep.txt and 

@date: 7/6/18
@author: Ben Gelman
'''

import os
import subprocess
from datetime import datetime

from muse import util, hashdeep, constant


def inner(dirpath, dryrun=True):
    tar_hash, main_hash = hashdeep.read_dir(dirpath)
    print 'tarhash'
    for i in tar_hash:
        print i
    print 'main_hash'
    for i in main_hash:
        print i
    match_files = main_hash.intersection(tar_hash)
    nonmatch_files = main_hash.difference(tar_hash)
    for size, md5, filepath in nonmatch_files:
        print 'nonmatch:  {}'.format(filepath)
    for size, md5, filepath in match_files:
        if os.path.isfile(filepath):
            print 'deleting:  {}'.format(filepath)
            if not dryrun:
                os.remove(filepath)
    for base, dirs, filenames in os.walk(dirpath):
        for d in dirs:
            path = os.path.join(dirpath, d)
            if not os.listdir(path):  # test for empty
                print 'deleting:  {}'.format(path)
                if not dryrun:
                    os.rmdir(path)
            else:
                print 'not empty: {}'.format(path)
        break  # don't recurse
    print 'completed'


def main_dedup(version, dryrun=True):
    """
    Walk through target directory and remove duplicate files
    """
    if dryrun:
        print 'Dryrun'
    with open('log/dedup/nonmatch_{}'.format(version), 'a') as nonmatch, \
            open('log/dedup/deleted_{}'.format(version), 'a') as deleted, \
            open('log/dedup/completed_{}'.format(version), 'a') as completed, \
            open('log/dedup/failed_{}'.format(version), 'a') as failed:

        for dirpath in constant.get_remaining():
            print 'Starting {} at {}'.format(dirpath,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            try:
                tar_hash, main_hash = hashdeep.read_dir(dirpath)
                match_files = main_hash.intersection(tar_hash)
                nonmatch_files = main_hash.difference(tar_hash)
                for size, md5, filepath in nonmatch_files:
                    nonmatch.write(filepath + '\n')
                for size, md5, filepath in match_files:
                    if os.path.isfile(filepath):
                        deleted.write(filepath + '\n')
                        print 'Deleting {}'.format(filepath)
                        if not dryrun:
                            os.remove(filepath)
                for base, dirs, filenames in os.walk(dirpath):
                    for d in dirs:
                        path = os.path.join(dirpath, d)
                        if not os.listdir(path):  # test for empty
                            print 'Deleting {}'.format(path)
                            if not dryrun:
                                os.rmdir(path)
                    break  # don't recurse
                completed.write('{}\n'.format(dirpath))
                print 'Completed'

            except KeyboardInterrupt:
                print 'Keyboard Interrupt. Exiting.'
                return
            except Exception as e:
                failed.write('{}\t{}\n'.format(dirpath, (str(e),)))


if __name__ == '__main__':
    import sys
    if len(sys.argv) not in (2, 3):
        print 'Usage: python main_dedup.py <name> <not_dry>'
        sys.exit()
    main_dedup(sys.argv[1], dryrun=(len(sys.argv) == 2))
