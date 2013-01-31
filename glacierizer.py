#!/usr/bin/python
#
# =============================================================================
#  Version: 1.1 (January 31, 2013)
#  Author: Jeff Puchalski (jpp13@cornell.edu)
#
# =============================================================================
# Copyright (c) 2013 Jeff Puchalski
#
# Permission is hereby granted, free of charge, to any person obtaining a copy 
# of this software and associated documentation files (the "Software"), to deal 
# in the Software without restriction, including without limitation the rights 
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
# copies of the Software, and to permit persons to whom the Software is 
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in 
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
# SOFTWARE.
# =============================================================================

"""Glacierizer:
Interface to Amazon Web Services' Glacier long-term data storage service.

Usage:
  glacierizer.py [options] <mode>
  
Mode:
  vault   create <vault name>
  vault   list
  archive read|write|delete <vault> <file1> [<file2> ...]
  job     <job_id> [<vault>]
    
Options:
  -c, --config=file   : load credentials from boto configuration file
  -h, --help          : display this help and exit
  -v, --verbose       : verbose output
  -V, --version       : display version number
  
Dependencies:
  boto (>=2.7.0) Interface to Amazon Web Services
    https://github.com/boto/boto
"""

version = '1.2'
verbose = False

import os, os.path, sys, getopt
import boto, sqlite3
from boto.pyami.config import Config

# SQL queries for vault operations
sql_vault_create = "CREATE TABLE IF NOT EXISTS {0} (filename text, archive_id text)"
sql_vault_write  = "INSERT INTO {0} VALUES ('{1}','{2}')"
sql_vault_read   = "SELECT * FROM {0} WHERE filename = '{1}'"
sql_vault_delete = "DELETE FROM {0} WHERE filename = '{1}'"

# SQL queries for managing jobs
sql_create_jobs = "CREATE TABLE IF NOT EXISTS jobs (id text, archive_id text, action text, status_code text, vault_name text)"
sql_job_write = "INSERT INTO jobs VALUES ('{1}','{2}','{3}','{4}','{5}')"
sql_job_read = "SELECT * FROM jobs WHERE job_id = '{1}'"

class Glacier(object):
    def __init__(self, access_key, secret_key):
        # Connect to Glacier
        if access_key and secret_key:
            self.conn = boto.connect_glacier(
                aws_access_key_id = access_key,
                aws_secret_access_key = secret_key
            )
        else:
            self.conn = boto.connect_glacier()
        
        # Connect to the SQLite database
        self.db = sqlite3.connect('glacier.db')
        self.db.row_factory = sqlite3.Row
        self.cursor = self.db.cursor()
        self.cursor.execute(sql_create_jobs)
            
    def vault_create(self, vault_name):
        self.conn.create_vault(vault_name)
        self.cursor.execute(sql_vault_create.format(vault_name))
        if verbose:
            print 'Created vault %s' % vault_name
        
    def vault_list(self, vault_name):
        vault = self.conn.get_vault(vault_name)
        job = vault.retrieve_inventory()
        self.cursor.execute(sql_job_write.format(
            job.id, 
            job.archive_id,
            job.action,
            job.status_code,
            vault_name
        ))
        if verbose:
            print 'Listing vault %s, job is %s' % (vault_name, job.id)

    def archive_read(self, vault_name, files):
        print 'Not yet implemented'
        return
    
    def archive_write(self, vault_name, files):
        for f in files:
            vault = self.conn.get_vault(vault_name)
            archive_id = vault.create_archive_from_file(f, description=f)
            self.cursor.execute(sql_vault_write.format(vault_name, f, archive_id))
            self.db.commit()
            if verbose: print 'Wrote %s (%s)' % (f, archive_id)
    
    def archive_delete(self, vault_name, files):
        for f in files:
            vault = self.conn.get_vault(vault_name)
            archive_id = self._get_archive_id(vault_name, f)
            vault.delete_archive(archive_id)
            self.cursor.execute(sql_vault_delete.format(vault_name, f))
            self.db.commit()
            if verbose: print 'Deleted %s (%s)' % (f, archive_id)

    def job(self, job_id, vault_name=None):
        # Use provided vault name
        if vault_name:
            result = self.conn.get_job_output(vault_name, job_id)
        # Look up vault name in jobs database
        else:
            self.cursor.execute(sql_job.format(job_id))
            job = self.cursor.fetchone()
            if not job:
                print >> sys.stderr, 'Could not find job for ID %s' % job_id
                sys.exit(2)
            result = self.conn.get_job_output(job.vault_name, job_id)
        print result
                 
    def _get_archive_id(self, vault_name, file_name):
        self.cursor.execute(sql_vault_read.format(vault_name, file_name))
        archive_id = self.cursor.fetchone()
        if not archive_id:
            print >> sys.stderr, 'Could not find archive ID for file %s in local database.' % file_name
            sys.exit(2)
        return archive_id
    
    def close():
        self.conn.close()
        self.db.close()
    
#------------------------------------------------------------------------------

def show_help():
    print >> sys.stdout, __doc__,

def show_usage(script_name):
    print >> sys.stderr, 'Usage: %s [options] <mode>' % script_name
    usage_modes = """Mode:
      vault   create <vault name>
      vault   list
      archive read|write|delete <vault> <file1> [<file2> ...]
      job     <job_id> [<vault>]
    """
    print >> sys.stderr, usage_modes

def main():
    script_name = os.path.basename(sys.argv[0])
    global verbose
    
    try:
        long_opts = ['help', 'config', 'verbose', 'version']
        opts, args = getopt.gnu_getopt(sys.argv[1:], 'hc:vV', long_opts)
    except getopt.GetoptError:
        show_usage(script_name)
        sys.exit(1)
    
    # Try to read .boto config from user home dir and current dir
    boto_cfg = None
    try:
        boto_cfg = Config(os.path.join(os.path.expanduser('~'), '.boto'))
        boto_cfg = Config('.boto')
    except:
        pass
    
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            show_help()
            sys.exit()
        elif opt in ('-c', '--config'):
            boto_cfg = Config(arg)
        elif opt in ('-v', '--verbose'):
            verbose = True
        elif opt in ('-V', '--version'):
            print script_name, ' version: ', version
            sys.exit(0)
    
    # Load the AWS key credentials
    try:
        access_key = boto_cfg.get('Credentials', 'aws_access_key_id')
        secret_key = boto_cfg.get('Credentials', 'aws_secret_access_key')
    except:
        print >> sys.stderr, 'Could not find .boto config file'
        show_usage(script_name)
        return
         
    # Connect to Glacier
    glacier = Glacier(access_key, secret_key)
    
    try:
        target = args[0]
        
        if target in ('v', 'vault', 'VAULT'):
            mode = args[1]
            vault_name = args[2]
            if mode in ('c', 'create', 'CREATE'):
                glacier.vault_create(vault_name)
            elif mode in ('l', 'list', 'LIST'):
                glacier.vault_list(vault_name)
                
        elif target in ('a', 'archive', 'ARCHIVE'):
            mode = args[1]
            vault_name = args[2]
            files = args[3:]
            if mode in ('r', 'read', 'READ'):
                glacier.archive_read(vault_name, files)
            elif mode in ('w', 'write', 'WRITE'):
                glacier.archive_write(vault_name, files)
            elif mode in ('d', 'delete', 'DELETE'):
                glacier.archive_delete(vault_name, files)
                
        elif target in ('j', 'job', 'JOB'):
            job_id = args[1]
            vault_name = args[2]
            glacier.job(job_id, vault_name)
        
        else:
            show_usage(script_name)
            sys.exit(2)
    except:
        pass
    
if __name__ == '__main__':main()