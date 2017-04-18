#!/home/jack/miniconda2/bin/python
# Logging wrapper for running automated scripts
# * Typically called by cron
# * Executes the command, captures all output to dated logfile
# * Logs the time, return code, and path to logfile in a master log
# * Upon request, returns a digest of all runs since last digest
import os
import datetime
import argparse
import json
import pandas

def run_job(jobname, jobdir, command):
    # Fully specify the logfilename
    dt_start = datetime.datetime.now()
    dt_string = dt_start.strftime('%Y-%m-%d-%H-%M-%S')

    ## Logfile
    logfiledir = os.path.join(log_root_directory, jobname)
    if not os.path.exists(logfiledir):
        os.mkdir(logfiledir)
    logfilename = os.path.join(logfiledir, 'output-%s' % dt_string)

    ## Create the full command string
    full_command = 'bash -l -c "cd %s; %s > %s 2>&1"' % (
        jobdir, command, logfilename,)

    ## Run
    returncode = os.system(full_command)
    dt_finished = datetime.datetime.now()

    ## Store the results
    duration = dt_finished - dt_start
    with file(master_logfile, 'a') as fi:
        fi.write('%s,%s,%s,%s,%s,%s,%s,%s\n' % (
            jobname,
            jobdir,
            command,
            str(dt_start),
            str(dt_finished),
            str(duration),
            str(returncode),
            logfilename,
        ))

## argparse
parser = argparse.ArgumentParser(description='Autolog a script.')
parser.add_argument('jobname')
args = parser.parse_args()
jobname = args.jobname

## Where everything happens
root_directory = '/home/jack/autolog'
if not os.path.exists(root_directory):
    raise IOError("autolog root does not exist")
log_root_directory = os.path.join(root_directory, 'logs')
if not os.path.exists(log_root_directory):
    os.mkdir(log_root_directory)
master_logfile = os.path.join(root_directory, 'master.log')

# Init the master_logfile if it doesn't exist
if not os.path.exists(master_logfile):
    with file(master_logfile, 'w') as fi:
        fi.write(
            'jobname,jobdir,command,start,finished,'
            'duration,returncode,logfile\n'
        )

## These could be associated with job name
if jobname == 'digest':
    time = datetime.datetime.now()
    logdf = pandas.read_csv(master_logfile)
    res = ''
    res += 'Digest at %s (%d jobs)\n' % (str(time), len(logdf))
    for jobid in logdf.index:
        res += 'Job %d: %s\n' % (jobid, logdf.loc[jobid, 'jobname'])
        res += 'Began: %s\n' % str(logdf.loc[jobid, 'start'])
        res += 'Ended: %s\n' % str(logdf.loc[jobid, 'finished'])
        res += 'Duration: %s\n' % str(logdf.loc[jobid, 'duration'])
        res += 'Return code: %s\n' % str(logdf.loc[jobid, 'returncode'])
        res += 'Output logfile: %s\n' % str(logdf.loc[jobid, 'logfile'])
        res += '\n'
    print res
        
else:
    with file('known_jobs.json') as known_jobs_fp:
        known_jobs = json.load(known_jobs_fp)

    if jobname not in known_jobs:
        raise ValueError("unknown job: %s" % jobname)
    else:
        command = known_jobs[jobname]['command']
        jobdir = known_jobs[jobname]['jobdir']

    run_job(jobname, jobdir, command)