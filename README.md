aletsch
=======

A script to interact with the AWS Glacier long-term data storage service.

Usage:

`python aletsch.py [options] <mode>`
  
Modes:

    vault    create|list|delete     <vault_name> [<job_id>]
    archive  read|write|list|delete <vault_name> <file1> [<file2> ...]
    job      output|status|remove   <job_id>
    
    Notes:
      - 'vault list' and 'archive read' both return a job ID of the submitted request. In order to retrieve
        the results you must wait for the Glacier job to complete, which takes about 4 hours. To know when the
        job has completed, you should set up publishing to an SNS topic for your vault and then set have the
        SNS topic send a notification (the most common way is e-mail or e-mail JSON). Then, you can use the
        'job output' command to get the results.
      - job_id can be a partial string (i.e., the first few characters of a job ID)
    
Options:

    -c, --config=file   : load credentials from the specified boto configuration file
    -h, --help          : display this help and exit
    -v, --version       : display version number
  
Dependencies:
* [boto](https://github.com/boto/boto) (>=2.7.0) Interface to Amazon Web Services.

