#!/usr/bin/python
#
# =============================================================================
#  Version: 1.5 (February 18, 2013)
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

import os, os.path, sys, argparse
import boto, sqlite3, json, textwrap
from boto import connect_glacier
from boto.glacier.exceptions import UnexpectedHTTPResponseError
from boto.pyami.config import Config

# Glacier connection
glacier = None

# Connect to the SQLite database
db = sqlite3.connect('aletsch.db')
db.row_factory = sqlite3.Row
cursor = db.cursor()

def init(access_key, secret_key):
    global glacier

    glacier = connect_glacier(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key
    )
    
    # Create the jobs table if it doesn't already exist
    cursor.execute("CREATE TABLE IF NOT EXISTS jobs (id text, action text, status_code text, vault_name text)")
    db.commit()
  
  
# SQLite queries
sql_vault_create = "CREATE TABLE IF NOT EXISTS {0} (filename text, archive_id text)"
sql_vault_delete = "DROP TABLE {0}"

sql_archive_create  = "INSERT INTO {0} VALUES ('{1}','{2}')"
sql_archive_read   = "SELECT * FROM {0} WHERE filename = '{1}'"
sql_archive_read_all = "SELECT * FROM {0}"
sql_archive_delete = "DELETE FROM {0} WHERE filename = '{1}'"

sql_jobs_create = "INSERT INTO jobs VALUES ('{0}','{1}','{2}','{3}')"
sql_jobs_read = "SELECT * FROM jobs WHERE id LIKE '{0}%'"
sql_jobs_read_all = "SELECT * FROM jobs"
sql_jobs_update = "UPDATE jobs SET status_code = '{0}' WHERE id = '{1}'"
sql_jobs_delete = "DELETE FROM jobs WHERE id LIKE '{0}'"


# Vault operations
def vault(args):
    if args.action == 'create':
        vault_create(args.vault_name)
    elif args.action == 'list':
        vault_list(args.vault_name)
    elif args.action == 'erase':
        vault_erase(args.vault_name, args.job_id)
    elif args.action == 'delete':
        vault_delete(args.vault_name)

def vault_create(vault_name):
    '''Creates a new vault.'''
    glacier.create_vault(vault_name)
    cursor.execute(sql_vault_create.format(vault_name))
    db.commit()
        
def vault_list(vault_name):
    '''Lists all of the archives in a vault. This returns a job ID that can later be queried using 'job output' to obtain the results, which in this case is the vault inventory.'''
    vault = glacier.get_vault(vault_name)
    job_id = vault.retrieve_inventory()
    job = vault.get_job(job_id)
    cursor.execute(sql_jobs_create.format(job.id, job.action, job.status_code, vault_name))
    db.commit()
    print job.id
    
def vault_erase(vault_name, job_id):
    '''Erases all archives in a vault. This operation takes some time, and results may not show up until the next inventory of the vault.'''
    vault = glacier.get_vault(vault_name)
    output = job_output(job_id)
    for entry in output['ArchiveList']:
        print entry['ArchiveId']
        vault.delete_archive(entry['ArchiveId']) 
    
def vault_delete(vault_name):
    '''Deletes a vault.'''
    glacier.delete_vault(vault_name)
    cursor.execute(sql_vault_delete.format(vault_name))
    db.commit()
    
    
# Archive operations
def archive(args):
    vault = glacier.get_vault(args.vault_name)
    
    if args.action == 'read':
        archive_read(vault, args.files)
    elif args.action == 'write':
        archive_write(vault, args.files)
    elif args.action == 'list':
        archive_list(vault)
    elif args.action == 'delete':
        archive_delete(vault, args.files)
    
def archive_read(vault, files):
    '''Initiates an archive retrieval job. This returns a job ID that can later be queried using 'job output' to obtain the results, which in this case is the archive contents.'''
    for f in files:
        if os.path.isfile(f):
            archive_id = _get_archive_id(vault.name, f)
        else: # try using the item as the archive ID
            archive_id = f
    job = vault.retrieve_archive(archive_id)
    cursor.execute(sql_jobs_create.format(job.id, job.action, job.status_code, vault.name))
    db.commit()
    print job.id
    
def archive_write(vault, files):
    '''Stores one or more files in a vault.'''
    for f in files:
        archive_id = vault.create_archive_from_file(f, description=f)
        cursor.execute(sql_archive_create.format(vault.name, f, archive_id))
        db.commit()

def archive_list(vault):
    '''Lists the managed files for a vault.'''
    cursor.execute(sql_archive_read_all)
    archives = cursor.fetchall()
    for (filename, archive_id) in archives:
        print "File:\t%s" % filename
        print "Archive ID:\t%s" % archive_id
        print "----"
    
def archive_delete(vault, files):
    '''Deletes an archive from a vault.'''
    for f in files:
        if os.path.isfile(f):
            archive_id = _get_archive_id(vault.name, f)
            cursor.execute(sql_archive_delete.format(vault.name, f))
            db.commit()
            vault.delete_archive(archive_id)
        else: # try using the item as the archive ID
            archive_id = f
            vault.delete_archive(archive_id)
      
# Archive helper function
def _get_archive_id(vault_name, file_name):
    '''Gets the archive ID for a file in the specified vault from the managed files database.'''
    cursor.execute(sql_archive_read.format(vault_name, file_name))
    archive_id = cursor.fetchone()
    if not archive_id:
        print >> sys.stderr, 'Could not find archive ID for file %s in local database.' % file_name
        sys.exit(2)
    return archive_id
      

# Job operations        
def job(args):
    if args.action == 'output':
        print json.dumps(job_output(args.job_id), indent=4, encoding='utf-8')
    elif args.action == 'status':
        job_status(args.job_id)
    elif args.action == 'remove':
        job_remove(args.job_id)
        
def job_output(job_id):
    '''Retrives the output for a job (vault inventory or archive retrieval).'''
    cursor.execute(sql_jobs_read.format(job_id))
    (id,action,status_code,vault_name) = cursor.fetchone()
    if not vault_name:
        print >> sys.stderr, 'Could not find vault name for job %s' % job_id
        sys.exit(2)
    vault = glacier.get_vault(vault_name)
    job = vault.get_job(id)
    return job.get_output()
        
def job_status(job_id=None):
    '''Prints out the status of all managed jobs.'''
    if job_id:
        cursor.execute(sql_jobs_read.format(job_id))
    else:
        cursor.execute(sql_jobs_read_all)
    jobs = cursor.fetchall()
    for (id, action, status_code, vault_name) in jobs:
        try:
            vault = glacier.get_vault(vault_name)
            job = vault.get_job(id)
            cursor.execute(sql_jobs_update.format(job.status_code, job.id))
            db.commit()
            print "Job:\t%s" % job.id
            print "Action:\t%s" % job.action
            print "Status:\t%s" % job.status_code
            print "Vault:\t%s" % vault.name
            print "----"
        except:
            cursor.execute(sql_jobs_delete.format(id))
            db.commit()

def job_remove(job_id):
    '''Removes a job from the managed jobs database.'''
    cursor.execute(sql_jobs_delete.format(job_id))
    db.commit()
            
#------------------------------------------------------------------------------

# Attempts to load the AWS access and secret keys from the .boto config
def get_aws_credentials(config_file):
    # Try to read .boto configuration from several places (later ones take precedence)
    try: # user home directory (~/.boto)
        boto_cfg = Config(os.path.join(os.path.expanduser('~'), '.boto'))
    except: pass
    try: # current directory (./.boto)
        boto_cfg = Config('.boto')
    except: pass
    try: # command line option (--config <file>)
        if config_file: boto_cfg = Config(config_file)
    except: pass

    # Load the AWS key credentials
    try:
        access_key = boto_cfg.get('Credentials', 'aws_access_key_id')
        secret_key = boto_cfg.get('Credentials', 'aws_secret_access_key')
    except:
        print >> sys.stderr, 'Could not find .boto config file'
        sys.exit(1)
        
    return (access_key, secret_key)
    
def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description='Interface to Amazon Glacier storage service.')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 1.4')
    parser.add_argument('--config', help='.boto configuration file')
    subparsers = parser.add_subparsers()
    
    parser_vault = subparsers.add_parser('vault')
    parser_vault.set_defaults(func=vault)
    parser_vault.add_argument('action', choices=['create','list','erase','delete'])
    parser_vault.add_argument('vault_name', help='Glacier vault name')
    parser_vault.add_argument('job_id', nargs='?', default=None, help='Job ID for inventory results (optional, used for erase)')
    
    parser_archive = subparsers.add_parser('archive')
    parser_archive.set_defaults(func=archive)
    parser_archive.add_argument('action', choices=['read','write','delete'])
    parser_archive.add_argument('vault_name', help='Glacier vault name')
    parser_archive.add_argument('files', nargs=argparse.REMAINDER, help='One or more files (can also be archive IDs)')
     
    parser_job = subparsers.add_parser('job')
    parser_job.set_defaults(func=job)
    parser_job.add_argument('action', choices=['output','status','remove'])
    parser_job.add_argument('job_id', nargs='?', default='', help='Glacier job ID (can be just the first few characters)')
    
    args = parser.parse_args()
    
    # Load the AWS credentials
    access_key, secret_key = get_aws_credentials(args.config)

    init(access_key, secret_key)
    args.func(args)

if __name__ == '__main__':main()