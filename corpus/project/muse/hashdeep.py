import os

from muse import util

def read_dir(dirpath):
    """
    Read hashdeep files in directory
    """
    uuid = os.path.basename(dirpath)
    if not util.is_uuid(uuid):
        raise ValueError('{} is not a uuid'.format(uuid))

    template = os.path.join(dirpath, '{}_hashdeep.txt')

    combine = []
    
    for f in ('code', 'meta', 'extra', 'main'):
        filename = template.format(f)
        if os.path.isfile(filename):
            if f == 'main':
                main = read_hashdeep(filename)
            else:
                combine.extend(read_hashdeep(filename))
        else:
            p = os.path.join(dirpath, uuid)
            if ((f == 'code' and os.path.exists(p + '_code.tgz'))
                    or (f == 'meta' and os.path.exists(p + '+_metadata.tgz'))
                    or (f == 'extra' and os.path.exists(p + '.tgz'))
                    or (f == 'main')):
                raise ValueError('{} does not exist'.format(filename))
        
    combine = remove_tmp(combine)  # combine tars and remove tmp
    tar_hash = set(combine)
    main_hash = set(main)

    return tar_hash, main_hash


def remove_tmp(hashdeep):
    """
    Remove /tmp directory from hashdeep
    """
    filtered = []
    if not hashdeep:
        return filtered

    first_path = hashdeep[0][2]
    index = first_path.find('/tmp')
    if index == -1:
        raise ValueError('{} does not have a tmp subdirectory'.format(
            first_path))

    for line in hashdeep:
        path = line[2]
        current_index = path.find('/tmp')
        if current_index != index:
            raise ValueError('{} does not match first line'.format(path))
        filtered.append(line[:2] + (path[:index] + path[index+len('/tmp'):],))

    return filtered


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
            line = line[:-1] if line.endswith('\n') else line
            if line:
                try:
                    size, md5, path = line.split(',', 2)
                    size = int(size)
                    if not util.is_md5(md5):
                        raise ValueError('{}:{} {} is not an md5'.format(
                            filepath, i, md5))
                    lines.append((size, md5, path))
                except ValueError as e:
                    # add line to previous line
                    size, md5, path = lines[-1]
                    lines[-1] = (size, md5, path + '\n' + line)
    return lines

