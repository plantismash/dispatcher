# This file is part of antiSMASH.
#
# antiSMASH is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# antiSMASH is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with antiSMASH.  If not, see <http://www.gnu.org/licenses/>.
'''smashctl job handling'''
import argparse
from collections import defaultdict
import json
import os
import sys
import shutil
from os import path
from dispatcher.models import Job
from dispatcher.mail import send_mail


def setup_job_options(subparsers):
    '''Shared setting for smashctl job'''
    p_job = subparsers.add_parser('job',
                                  help="Manipulate or show jobs")
    p_job.add_argument('-j', '--jobtype',
                       default='antismash4',
                       help="Job type to work on")
    p_job.add_argument('-w', '--workdir', dest="workdir",
                       help="Path to working directory that contains the uploaded sequences",
                       default="upload")
    job_subparsers = p_job.add_subparsers(title='job-related commands')

    p_job_list = job_subparsers.add_parser('list',
                                           help="List jobs")
    p_job_list.add_argument('--status', dest='status', nargs='+',
                            default=['running', 'pending'],
                            help="Specify the job status")
    p_job_list.add_argument('--pretty', dest='pretty', default='simple',
                            choices=['fancy', 'simple', 'multiline'],
                            help="Print fancy, simple or multiline output")
    p_job_list.set_defaults(func=job_list)

    p_job_submit = job_subparsers.add_parser('submit',
                                             help='Submit a new job')
    p_job_submit.add_argument('--taxon', dest='taxon',
                              choices=['bacteria', 'fungi'], default='bacteria',
                              help="Taxonomic classification of the input sequence. Default: %(default)s")
    p_job_submit.add_argument('-i', '--inclusive', dest='inclusive',
                              action='store_true', default=False,
                              help="Run ClusterFinder search")
    p_job_submit.add_argument('-s', '--smcogs', dest='smcogs',
                              action='store_true', default=False,
                              help="Run smCOG analysis")
    p_job_submit.add_argument('-a', '--all-orfs', dest='all_orfs',
                              action='store_true', default=False,
                              help="Analyze all possible ORFs")
    p_job_submit.add_argument('-c', '--clusterblast', dest='clusterblast',
                              action='store_true', default=False,
                              help="Run clusterblast analysis")
    p_job_submit.add_argument('-k', '--knownclusterblast', dest='knownclusterblast',
                              action='store_true', default=False,
                              help="Run knownclusterblast analysis")
    p_job_submit.add_argument('--subclusterblast', dest='subclusterblast',
                              action='store_true', default=False,
                              help="Run subclusterblast analysis")
    p_job_submit.add_argument('--tta', dest='tta',
                              action='store_true', default=False,
                              help="Detect TTA codons")
    p_job_submit.add_argument('--molecule', dest="molecule",
                              choices=['nucl', 'prot'], default='nucl',
                              help="Select molecule type of input")
    p_job_submit.add_argument('--fullhmmer', dest='fullhmmer',
                              action='store_true', default=False,
                              help="Run full genome PFAM analysis")
    p_job_submit.add_argument('--cassis', dest='cassis',
                              action='store_true', default=False,
                              help="Run CASSIS algorithm")
    p_job_submit.add_argument('--gff3', dest='gff3',
                              default=argparse.SUPPRESS,
                              help="Feature annoations in GFF3 format")
    p_job_submit.add_argument('sequence',
                              help='Sequence file to run')
    p_job_submit.set_defaults(func=job_submit)

    p_job_cancel = job_subparsers.add_parser('cancel',
                                             help="Cancel a pending job")
    p_job_cancel.add_argument('--delete', dest='delete',
                              action='store_true', default=False,
                              help='Delete files related to job')
    p_job_cancel.add_argument('--force', dest='force',
                              action='store_true', default=False,
                              help='Also cancel non-pending jobs')
    p_job_cancel.add_argument('--reason', dest='reason',
                              default='Manual interruption',
                              help='Specify a reason for canceling the job')
    p_job_cancel.add_argument('--status', dest='status',
                              default='canceled',
                              help='status to leave the job in (default: canceled)')
    p_job_cancel.add_argument('--send-mail', dest="send_mail",
                              action='store_true', default=False,
                              help='Send a mail to the submitter if email address was provided')
    p_job_cancel.add_argument('uid',
                              help="Identifier of pending job to cancel")
    p_job_cancel.set_defaults(func=job_cancel)

    p_job_restart = job_subparsers.add_parser('restart',
                                              help="Restart a job")
    p_job_restart.add_argument('uid',
                               help="Identifier of job to restart")
    p_job_restart.add_argument('-l', '--long-running', dest="long_running",
                               action="store_true", default=False,
                               help="Put job into the long-running queue")
    p_job_restart.set_defaults(func=job_restart)

    p_job_show = job_subparsers.add_parser('show',
                                           help="Show job details")
    p_job_show.add_argument('--pretty', dest='pretty', default='multiline',
                            choices=['simple', 'multiline', 'json'],
                            help="Print simple or multiline output")

    p_job_show.add_argument('uid',
                            help="Identifier of job to show")
    p_job_show.set_defaults(func=job_show)


def job_list(args):
    '''Handle smashctl job list'''
    redis_store = args.redis_store
    job_ids = []
    for status in args.status:
        job_ids.extend(redis_store.lrange('jobs:%s' % status, 0, -1))
    header = None
    footer = None
    if args.pretty == 'fancy':
        header = '%s\n' % (80 * '=')
        header += "| %-36s | %-10s | %-24s |\n" % ('uuid', 'jobtype', 'status')
        header += "|-%s-|-%s-|-%s-|" % (36 * '-', 10 * '-', 24 * '-')
        template = "| %(uid)s | %(jobtype)-10s | %(status)-24.24s |"
        footer = '%s' % (80 * '=')
    elif args.pretty == 'simple':
        template = "%(uid)s\t%(dispatcher)s\t%(email)s\t%(added)s\t%(last_changed)s\t%(filename)s%(download)s\t%(status)s"
    else:
        template = """Job %(uid)s
    Jobtype: %(jobtype)s
    Status:  %(status)s
    Added: %(added)s
    Last Changed: %(last_changed)s
"""

    if header is not None:
        print header
    for job_id in job_ids:
        job_ = redis_store.hgetall("job:%s" % job_id)
        if job_ is None:
            continue
        # new jobs don't have a lot of fields anymore
        job = defaultdict(str)
        job.update(job_)
        if 'uid' not in job:
            job['uid'] = job_id
        print template % job
    if footer is not None:
        print footer


def job_submit(args):
    '''Handle smashctl job submit'''
    redis_store = args.redis_store
    opts = vars(args)
    job = Job(**opts)
    job.filename = path.basename(args.sequence)

    jobdir = path.abspath(path.join(args.workdir, job.uid))
    os.mkdir(jobdir)
    shutil.copy(args.sequence, jobdir)
    if 'gff3' in args:
        shutil.copy(args.gff3, jobdir)
        job.gff3 = path.basename(args.gff3)

    print "Submitting job %r (%s)" % (job.uid, job.jobtype)
    redis_store.hmset("job:{}".format(job.uid), job.get_dict())
    redis_store.lpush("jobs:queued", job.uid)


def job_cancel(args):
    '''Handle smashctl job cancel'''
    redis_store = args.redis_store
    job_id = "job:{}".format(args.uid)
    if not redis_store.exists(job_id):
        print "No such job: {}".format(args.uid)

    job = Job(**redis_store.hgetall(job_id))
    old_status = job.status.split(':', 1)[0]
    if not args.force and old_status not in ('pending', 'canceled'):
        print "Cannot cancel job in status '%s'" % job.status
        sys.exit(1)

    if args.delete and job.status == 'pending':
        jobdir = path.abspath(path.join(args.workdir, job.uid))
        try:
            shutil.rmtree(jobdir, False)
            print "Deleted job %r files." % job.uid
        except OSError as err:
            print >>sys.stdout, "Failed to delete job %r files: %s" % (job.uid, err)

    job.status = "%s: %s" % (args.status, args.reason)
    redis_store.hset(job_id, 'status', job.status)
    # Naming for pending job queue is inconsistent, unfortunately
    if old_status == "pending":
        old_status = "queued"
    redis_store.lrem("jobs:%s" % old_status, job.uid, -1)
    redis_store.lpush("jobs:%s" % args.status, job.uid)
    if args.send_mail:
        try:
            send_mail(job)
        except Exception as err:
            print "failed to send mail: %s" % err

    print "Canceled job %r (%s)" % (job.uid, job.status)


def job_restart(args):
    '''handle smashctl job restart'''
    redis_store = args.redis_store
    job_id = "job:{}".format(args.uid)
    if not redis_store.exists(job_id):
        print "No such job: {}".format(args.uid)

    job = Job(**redis_store.hgetall(job_id))
    old_status = job.status.split(':', 1)[0]
    if old_status not in ('running', 'canceled', 'done', 'failed', 'pending'):
        print "Cannot restart job in status '%s'" % job.status
        sys.exit(1)

    job.status = "pending"
    redis_store.hset(job_id, 'status', job.status)

    # also re-download the input file
    if job.download != '':
        job.filename = ''
        redis_store.hset(job_id, 'filename', job.filename)

    if args.long_running:
        queue = "jobs:timeconsuming"
    else:
        queue = "jobs:queued"

    redis_store.lrem("jobs:%s" % old_status, job.uid, -1)
    redis_store.rpush(queue, job.uid)
    print "restarted job %r" % job.uid


def job_show(args):
    '''Handle smashctl job show'''
    redis_store = args.redis_store
    job_struct = redis_store.hgetall("job:{}".format(args.uid))
    job = Job(**job_struct)
    if job == {}:
        print "No such job: {}".format(args.uid)
        return

    if args.pretty == 'json':
        print json.dumps(job_struct, sort_keys=True, indent=4, separators=(',', ': '))
        return

    if args.pretty == 'multiline':
        template = """Job '%(uid)s'
    Jobtype: %(jobtype)s
    Dispatcher: %(dispatcher)s
    Email: %(email)s
    Filename: %(filename)s
    GFF3 file: %(gff3)s
    Download: %(download)s
    Added: %(added)s
    Last changed: %(last_changed)s
    Dispatcher: %(dispatcher)s
    Geneclustertypes: %(geneclustertypes)s
    Taxon: %(taxon)s
    all orfs: %(all_orfs)s
    from pos: %(from_pos)s
    to pos: %(to_pos)s
    inclusive: %(inclusive)s
    smcogs: %(smcogs)s
    clusterblast: %(clusterblast)s
    subclusterblast: %(subclusterblast)s
    fullhmmer: %(fullhmmer)s
    status: '%(status)s'
"""
    else:
        template = "%(uid)s\t%(dispatcher)s\t%(added)s\t%(last_changed)s\t%(status)r"
    print template % job.get_dict()
