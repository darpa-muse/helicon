import os
import re

UUID_PATTERN = '^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$'
MD5_PATTERN = '^[0-9a-f]{32}$'


def is_uuid(string):
    """
    Return whether string is a uuid
    """
    return re.match(UUID_PATTERN, string) is not None


def is_md5(string):
    """
    Return whether string is an md5
    """
    return re.match(MD5_PATTERN, string) is not None


def walk_uuids(basedir):
    """
    Generate a sorted list of all paths under basedir with uuid as directory

    Stop recursing once uuid is found in a subdir.
    """
    
    if basedir.endswith('/'):
        basedir = basedir[:-1]
        if not basedir:
            raise ValueError('do not target /')

    basename = os.path.basename(basedir)
    if is_uuid(basename):
        yield basename
    else:
        for root, directories, files in os.walk(basedir):
            directories.sort()
            uuids = [x for x in directories if is_uuid(x)]
            for u in uuids:
                directories.remove(u)
                yield os.path.join(root, u)


def is_octet_dir(directory, octets=2):
    octets = int(octets)
    if octets < 1:
        raise ValueError('octets must be > 0')

    subdirs = os.listdir(directory)
    if len(subdirs) != 16**octets:
        print 'there are not {} octets'.format(16**octets)
        return False
    for subdir in subdirs:
        if len(subdir) != octets:
            print 'subdir {} is not a octet of size {}'.format(subdir, octets)
            return False
        if any([x not in '01234567890abcdef' for x in subdir]):
            print 'subdir {} is not a valid octet'.format(subdir)
            return False
    return True



