"""
High level script for working through the entire corpus
"""

import os

import pyaccumulo

from accumulo import util


class FileReader(object):
    """
    Reads files from Accumulo
    """

    def __init__(self, host='192.168.0.6', port=50096, user='root',
                 password='twosix', file_table='files', 
                 project_table='project', tmp_dir='tmp'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.file_table = file_table
        self.conn = None

    def connect(self):
        if self.conn is None:
            self.conn = pyaccumulo.Accumulo(host=self.host, port=self.port,
                    user=self.user, password=self.password)
            if not self.conn.table_exists(self.file_table):
                raise ValueError('{} table does not exist'.format(
                    self.file_table))
        else:
            try:
                self.conn.close()
            except:
                pass
            self.conn = pyaccumulo.Accumulo(host=self.host, port=self.port,
                user=self.user, password=self.password)
        return self

    def _check_connection(self):
        if self.conn is None:
            raise ValueError('must call connect()')

    def prefix(self, pre):
        """
        Return md5s starting with the prefix
        """
        self._check_connection()

        hashes = set()
        r = pyaccumulo.Range(srow=pre, erow=pre+'}')
        for entry in self.conn.scan(self.file_table, scanrange=r, 
                cols=[['file|size']]):
            hashes.add(entry.row)
        
        return sorted(list(hashes))

    def range(self, start, end):
        """
        Return md5s in the range of (start, end)
        """
        self._check_connection()

        hashes = set()
        r = pyaccumulo.Range(srow=start, erow=end)
        for entry in self.conn.scan(self.file_table, scanrange=r, 
                cols=[['file|size']]):
            hashes.add(entry.row)
        
        return sorted(list(hashes))


    def range_uuid_name(self, start, end):
        """
        Return a list of tuples from md5 to filename
        """
        self._check_connection()

        md5 = None
        r = pyaccumulo.Range(srow=start, erow=end)
        for entry in self.conn.scan(self.file_table, scanrange=r, 
                cols=[['file|path']]):
            if entry.row != md5:
                md5 = entry.row
                filepath = entry.cq
                uuid = filepath[:36]
                filename = os.path.basename(filepath)
                yield md5, uuid, filename
        

    def get(self, md5):
        self._check_connection()

        util.check_md5(md5) 
        r = pyaccumulo.Range(srow=md5, erow=md5, scf='file{', ecf='file}')
        entries = [x for x in self.conn.scan(self.file_table, scanrange=r)]
        if len(entries) == 0:
            return None

        projects = []
        paths = []
        size = None
        tags = False
        for entry in self.conn.scan(self.file_table, scanrange=r):
            if entry.cf == 'file|path':
                paths.append(entry.cq)
            elif entry.cf == 'file|project':
                projects.append(entry.cq)
            elif entry.cf == 'file|size' and entry.cq == 'size':
                size = int(entry.val)
            elif entry.cf == 'twosix' and entry.cq == 'tags':
                tags = bool(entry.val)
            elif entry.cf == 'file|content' and entry.cq == 'content':
                content = entry.val
            else:
                raise ValueError('cf {}, cq {} not recognized'.format(
                    entry.cf, entry.cq))

        return projects, paths, size, content, tags

    def content(self, md5):
        util.check_md5(md5)


class DataGatherer(object):
    """
    Unpacks, extracts, and ingests files from code
    """
    def __init__(self, host='100.120.0.6', port=50096, user='root',
                 password='twosix', file_table='files', 
                 project_table='project', tmp_dir='tmp'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.file_table = file_table
        self.project_table = project_table
        self._connect()
        self._build()

    def _connect(self):
        self.conn = pyaccumulo.Accumulo(host=self.host, port=self.port,
                user=self.user, password=self.password)
        if not self.conn.table_exists(self.file_table):
            self.conn.create_table(self.file_table)
        if not self.conn.table_exists(self.project_table):
            self.conn.create_table(self.project_table)

        self.wr = self.conn.create_batch_writer(
                self.file_table, max_memory=1024*1024)

    def reconnect(self):
        try:
            self.wr.close()
        except:
            pass
        try:
            self.conn.close()
        except:
            pass
        self.conn = pyaccumulo.Accumulo(host=self.host, port=self.port,
                user=self.user, password=self.password)
        self.wr = self.conn.create_batch_writer(
                self.file_table, max_memory=1024*1024)

    def _build(self):
        print 'building hash set'
        self.hashes = set()
        for entry in self.conn.batch_scan(self.file_table, numthreads=1,
                                          cols=[['file|size']]):
            self.hashes.add(entry.row)
            
        print 'building project set'
        self.projects = set()
        for entry in self.conn.batch_scan(self.project_table, numthreads=1):
            self.projects.add(entry.row)

    def write_files(self, uuid, md5_list, size_list, path_list, 
                    full_path_list):
        if not (len(md5_list) == len(size_list) == 
                len(path_list) == len(full_path_list)):
            raise ValueError('lists are of different length')
        
        #self.wr = self.conn.create_batch_writer(self.file_table)
        for entry in zip(md5_list, size_list, path_list, full_path_list):
            self._write_file(uuid, *entry)
        #self.wr.close()

    def write_file(self, uuid, md5, size, path, full_path):
        self.write_files(self, uuid, [md5], [size], [path], [full_path])

    def _write_file(self, uuid, md5, size, path, full_path):
        with open(full_path, 'rb') as f:
            data = f.read()

        util.check_uuid(uuid)
        util.check_md5(md5)
        if size != len(data):
            raise ValueError('size of file {}: {} != {}'.format(
                    path, len(data), size))
        util.check_md5_hash(data, md5)
        if not path.startswith(uuid + '/'):
            if path.startswith('latest/') or path.startswith('content'):
                path = uuid + '/' + path
            else:
                raise ValueError('path does not start with uuid or svn/git')
        

        m = pyaccumulo.Mutation(md5)
        m.put(cf='file|project', cq=uuid)
        m.put(cf='file|path', cq=path)
        if md5 not in self.hashes:
            m.put(cf='file|size', cq='size', val=str(size))
            m.put(cf='file|content', cq='content', val=data)
            m.put(cf='twosix', cq='tags')  # false
            self.hashes.add(md5)
        #m.put(cf='file|count', cq='count', value=TODO)
        #m.put(cf='twosix|tag', cq='recursion', val='0.443')
        self.wr.add_mutation(m)

    def read(self, md5):
        util.check_md5(md5) 
        r = pyaccumulo.Range(srow=md5, erow=md5, scf='file{', ecf='file}')
        entries = [x for x in self.conn.scan(self.file_table, scanrange=r)]
        if len(entries) == 0:
            return None

        projects = []
        paths = []
        size = None
        for entry in entries:
            if entry.cf == 'file|path':
                paths.append(entry.cq)
            elif entry.cf == 'file|project':
                projects.append(entry.cq)
            elif entry.cf == 'file|size' and entry.cq == 'size':
                size = int(entry.val)

    def add_project(self, uuid):
        util.check_uuid(uuid)
        m = pyaccumulo.Mutation(uuid)
        m.put(cf="", cq="")
        self.conn.write(self.project_table, m)
        self.projects.add(uuid)
