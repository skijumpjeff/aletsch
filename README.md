glacierizer
===========

A script to interact with the AWS Glacier long-term data storage service.

Usage:

`glacierizer.py [options] <mode>`
  
Modes:

    vault    create|list|delete <vault_name>  
    archive  read|write|delete <vault_name> <file1> [<file2> ...]
    job      output|status <job_id*>
    
    *job_id can be partial (i.e., first few characters)
    
Options:

    -c, --config=file   : load credentials from boto configuration file
    -h, --help          : display this help and exit
    -v, --version       : display version number
  
Dependencies:
* [boto](https://github.com/boto/boto) (>=2.7.0) Interface to Amazon Web Services.

