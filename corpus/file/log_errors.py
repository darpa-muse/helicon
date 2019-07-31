"""
Pull apart the log files into types of errors
"""

import os
import collections

from reversecrowd.accumulo import util


log_name = 'log/accumulo.log'


if os.path.isfile(log_name):
    with open(log_name) as f:
        lines = [line.rstrip('\n') for line in f]

    start = 'ERROR:__main__:uuid '
    L = len(start)
    error_index = []
    uuids = []
    errors = []
    for i, line in enumerate(lines):
        if line.startswith(start):
            error_index.append(i)
            uuid = line[L:L + 36]
            util.check_uuid(uuid)
            uuids.append(uuid)
            error = line[L + 36:]
            if not error.startswith(': '):
                raise ValueError('error does not start with ": "')
            errors.append(error[2:])
    error_index.append(None)
    traces = []
    for i in range(1, len(error_index)):
        traces.append(lines[error_index[i - 1]:error_index[i]])

failed = {
    'not a file': [],
    'ascii codec': [],
    'local size': [],
    'size > max': [],
    'connection reset': [],
    'broken pipe': [],
    'createwriter': [],
    'file exists': [],
    'tokens': [],
    'literal': [],
    'interrupted': [],
    'unknown': [],
}
for error, uuid in zip(errors, uuids):
    if error.endswith('_code.tgz is not a file'):
        failed['not a file'].append(uuid)
    elif error.startswith("'ascii' codec can't decode"):
        failed['ascii codec'].append(uuid)
    elif error == "local variable 'size' referenced before assignment":
        failed['local size'].append(uuid)
    elif error.endswith('> 134217728 max') and '_code.tgz size ' in error:
        failed['size > max'].append(uuid)
    elif error == '[Errno 104] Connection reset by peer':
        failed['connection reset'].append(uuid)
    elif error == '[Errno 32] Broken pipe':
        failed['broken pipe'].append(uuid)
    elif error == 'createWriter failed: unknown result':
        failed['createwriter'].append(uuid)
    elif error.startswith("[Errno 17] File exists: 'tmp/"):
        failed['file exists'].append(uuid)
    elif error == 'len(tokens) must be at least 3':
        failed['tokens'].append(uuid)
    elif error.startswith('invalid literal for int() with base 10: '):
        failed['literal'].append(uuid)
    elif error == '[Errno 4] Interrupted system call':
        failed['interrupted'].append(uuid)
    else:
        failed['unknown'].append(uuid)

for k in failed.keys():
    failed[k] = set(failed[k])
