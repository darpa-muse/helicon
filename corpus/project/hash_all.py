"""
Main script that goes through projects, hashes, deduplicates (deletes)...
"""

import os
import shutil
import subprocess

from muse import util, tars, constant


def extract_project(tarpath, tmp_dir):
    try:
        command = 'tar -zxf {} -C {}'.format(tarpath, tmp_dir).split()
        subprocess.call(command)
    except subprocess.CalledProcessError as e:
        raise ValueError('{} not extracted correctly: {}'.format(
            project_dir, e))

        
def process_dir(uuid_dir, tmp_dir='tmp'):
    # ensure full path
    if not tmp_dir.startswith('/'):
        tmp_dir = os.path.join(os.getcwd(), tmp_dir)

    if not os.path.isdir(tmp_dir):
        raise ValueError('tmp_dir {} does not exist'.format(tmp_dir))
    if not os.access(tmp_dir, os.W_OK):
        raise ValueError('tmp_dir {} cannot be written to'.format(tmp_dir))
    if not os.access(uuid_dir, os.W_OK):
        raise ValueError('{} cannot be written to'.format(uuid_dir))

    # create uuid directory 
    uuid = os.path.basename(uuid_dir)
    target_base = os.path.join(tmp_dir, uuid)

    for tarball, hashname in [
            ('{}_code.tgz'.format(uuid), 'code_hashdeep.txt'),
            ('{}_metadata.tgz'.format(uuid), 'meta_hashdeep.txt'),
            ('{}.tgz'.format(uuid), 'extra_hashdeep.txt'),
            ]:
        tarpath = os.path.join(uuid_dir, tarball)
        hashpath = os.path.join(uuid_dir, hashname)
        if os.path.exists(hashpath):
            continue
        if not os.path.exists(tarpath):
            continue  # nothing to hash

        
        # remove the target directory
        try:
            command = 'rm -rf {}'.format(target_base).split()
            subprocess.call(command)
        except subprocess.CalledProcessError as e:
            raise ValueError(
                '{} not deleted correctly: {}'.format(target_base, e))
        os.mkdir(target_base)
        
        # untar into tmp
        extract_project(tarpath, target_base) 

        # write size
        try:
            command = 'du -sk {}'.format(target_base).split()
            string = subprocess.check_output(command)
            with open('log/size/kilobyte_size.txt', 'a') as f:
                f.write('{}\t{}'.format(tarpath, string))
        except subprocess.CalledProcessError as e:
            raise ValueError('{} size not successful: {}'.format(
                target_base, e))
        
        # hash directory as if it was in tmp on the SAN
        string = tars.hash_dir(target_base)
        string = string.replace(target_base, os.path.join(uuid_dir, 'tmp'))
        near_hash = os.path.join(target_base, hashname)
        with open(near_hash, 'w') as f:  # write and then move the file
            f.write(string)
        shutil.move(near_hash, hashpath)

        # remove the target directory
        try:
            command = 'rm -rf {}'.format(target_base).split()
            subprocess.call(command)
        except subprocess.CalledProcessError as e:
            raise ValueError(
                '{} not deleted correctly: {}'.format(target_base, e))

    # process main hash
    try:
        command = 'rm -rf {}'.format(target_base).split()
        subprocess.call(command)
    except subprocess.CalledProcessError as e:
        raise ValueError(
            '{} not deleted correctly: {}'.format(target_base, e))
    os.mkdir(target_base)
    
    hashname = 'main_hashdeep.txt'
    hashpath = os.path.join(uuid_dir, hashname)
    if not os.path.exists(hashpath):
        string = tars.hash_dir(uuid_dir)
        near_hash = os.path.join(target_base, hashname)
        with open(near_hash, 'w') as f:  # write and then move the file
            f.write(string)
        shutil.move(near_hash, hashpath)

    try:
        command = 'rm -rf {}'.format(target_base).split()
        subprocess.call(command)
    except subprocess.CalledProcessError as e:
        raise ValueError(
            '{} not deleted correctly: {}'.format(target_base, e))


def main(tmp_dir='tmp'):
    for i, uuid_dir in enumerate(constant.get_remaining()):
        with open('log/errors.txt', 'a') as f:
            print uuid_dir, i
            f.write(uuid_dir + '\n')
            try:
                process_dir(uuid_dir, tmp_dir)
            except KeyboardInterrupt:
                print 'Exiting'
                break
            except Exception as e:
                print e
                f.write(str(e)+'\n\n')
        

if __name__ == '__main__':
    main()
