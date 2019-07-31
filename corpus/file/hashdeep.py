"""
Functions for parsing hashdeep file
"""


from accumulo import util


def parse(filename, uuid):
    util.check_uuid(uuid)
    with open(filename) as f:
        header = f.next()
        if header != '%%%% size,md5,filename\n':
            raise ValueError('header {} is not recognized'.format(header))
        lines = [line.rstrip() for line in f if line]  # ignore empty lines
        lines = [line for line in lines if line]
        # parse paths
        if not len(lines):
            raise ValueError('no lines in file {}'.format(filename))

    tokens = []
    for i, line in enumerate(lines):
        token = line.split(',', 2)  # any commas after the 2nd are in the path
        try:
            if len(token) < 3:
                raise ValueError('{} < 3 tokens in line'.format(len(token)))
            size, md5, path = token
            size = int(size)
            util.check_md5(md5)
        except ValueError as e:
            raise ValueError('line {}, value {}: {}'.format(i + 1, line, e))
        tokens.append((size, md5, path))
    sizes, md5s, paths = zip(*tokens)
    
    path.find(uuid) + len(uuid)
    ind = path.find(uuid)
    if ind == -1:
        raise ValueError('cannot find uuid {}'.find(uuid))
    start = ind + len(uuid)
    if path[start:].startswith('/tmp/'):
        suffix = '/tmp/'
    elif path[start:].startswith('/'):
        suffix = '/'
    else:
        raise ValueError('path does not have "/" after uuid')
    prefix = path[:start] + suffix
    
    length = len(prefix)
    for i, p in enumerate(paths):
        if not p.startswith(prefix):
            raise ValueError('line {} does not start with {}'.format(
                i, prefix))
   
    paths = [p[length:] for p in paths]
    for i, p in enumerate(paths):
        if not p:
            raise ValueError('line {} has an empty path'.format(i))
            
    return sizes, md5s, paths


def yield_uuid_dirs(basedir):
    for root, dirs, files in os.walk(basedir):
        # if dirs has <uuid>
        # TODO
        paths, uuids
        yield path


def parse_dir(uuid_dir):
    uuid = os.path.basename(uuid_dir)
    code = os.path.join(uuid_dir, 'code_hashdeep.txt')
    if not os.path.isfile(code):
        raise ValueError('{} is not a file'.format(code))

    sizes, md5s, paths = parse(code, uuid)
    
    # TODO: filter out .svn and .git
    return sizes, md5s, paths


def parse_tree(basedir):
    pass
