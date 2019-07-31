import logging
log_name = 'log/accumulo.log'
logging.basicConfig(filename=log_name, level=logging.DEBUG)
log = logging.getLogger(__name__)
import time
import json
import os

import numpy as np

from accumulo import accumulo, extract, util


with open('conf/accumulo.json') as f:
    config = json.load(f)

# get all uuids in SAN
with open('data/uuids.json') as f:
    uuids = json.load(f)


# get all failed uuids in log
if os.path.isfile(log_name):
    with open(log_name) as f:
        failed = []
        start = 'ERROR:__main__:uuid '
        index = len(start)
        for line in f:
            if line.startswith(start): 
                uuid = line[index:index + 36]
                try:
                    util.check_uuid(uuid)
                    failed.append(uuid)
                except ValueError:
                    pass
    failed = set(failed)

import log_errors


gatherer = accumulo.DataGatherer(**config)
start = time.time()
counter = 0
for i, uuid in enumerate(uuids):
    mid = time.time()
    if (np.log2(i) % 1) == 0.0:
        print '\n**** {} uuids in {} seconds ****\n'.format(i, mid - start)

    if uuid in gatherer.projects:
        #print 'skipped'
        continue
    if uuid in log_errors.failed['size > max']:
        #print 'skipped failure (size)'
        continue
    if uuid in log_errors.failed['not a file']:
        #print 'skipped failure (not a file)'
        continue

    print '{}'.format(i).rjust(6), '{}'.format(uuid),
    try:
        with extract.Extractor(uuid) as extractor:
            extractor.extract()
            tokens = extractor.get_tokens()
            if tokens:
                size_list, md5_list, path_list, full_path_list = zip(*tokens)
                print 'files: {}, size: {}'.format(len(tokens),
                                                   sum(size_list)),
                gatherer.write_files(uuid, md5_list, size_list, path_list,
                                     full_path_list)
            else:
                print 'files: 0, size: 0',
            gatherer.add_project(uuid)
        print 'success',
    except KeyboardInterrupt:
        print 'interrupted'
        print 'Caught KeyboardInterrupt. Saving...'
        break
    except Exception as e:
        print 'failed',
        if '[Errno 32] Broken pipe' in str(e):
            print 'due to broken pipe (likely Proxy down)'
            break
        log.exception('uuid {}: {}'.format(uuid, e))
        failed.add(uuid)
    end = time.time()
    print 'in {} seconds'.format(end - mid)
    counter += 1
    if counter % 1024 == 0:
        print 'Reconnecting...'
        gatherer.reconnect()
