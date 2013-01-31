glacierizer
===========

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
