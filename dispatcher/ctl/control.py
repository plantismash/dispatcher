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
import sys


def setup_control_options(subparsers):
    p_control = subparsers.add_parser('control',
                                      help="Control the dispatchers")
    control_subparsers = p_control.add_subparsers(title='job-related commands')

    p_control_list = control_subparsers.add_parser('list',
                                                   help="List dispatchers")
    p_control_list.add_argument('--pretty',
                                dest='pretty', default='standard',
                                choices=['simple', 'standard'],
                                help='Modify the output style')
    p_control_list.set_defaults(func=control_list)

    p_control_stop = control_subparsers.add_parser('stop',
                                                   help='Stop a dispatcher')
    p_control_stop.add_argument('name',
                                help='Name of dispatcher to stop')
    p_control_stop.set_defaults(func=control_stop)

    p_control_scale = control_subparsers.add_parser('scale',
            help="Scale the number of jobs for a dispatcher")
    p_control_scale.add_argument('name',
            help="Name of the dispatcher to scale")
    p_control_scale.add_argument('jobs', type=int,
            help="Number of jobs to run on the dispatcher")
    p_control_scale.set_defaults(func=control_scale)


def control_list(args):
    redis_store = args.redis_store
    dispatcher_ids = redis_store.keys("control:*")
    dispatcher_ids.sort()
    for dispatcher_id in dispatcher_ids:
        dispatcher = redis_store.hgetall(dispatcher_id)
        # TODO: Can be removed once all dispatchers export running_jobs
        if 'running_jobs' not in dispatcher:
            dispatcher['running_jobs'] = '?'

        if args.pretty == 'simple':
            template = "%(name)s\t%(running)s\t%(stop_scheduled)s\t%(status)s\t%(max_jobs)s\t%(running_jobs)s"
        else:
            template = """%(name)s
    running: %(running)s
    stopping: %(stop_scheduled)s
    status: %(status)s
    max_jobs: %(max_jobs)s
    running_jobs: %(running_jobs)s"""

        print template % dispatcher


def control_stop(args):
    redis_store = args.redis_store
    if args.name == "all":
        dispatcher_ids = redis_store.keys("control:*")
    else:
        dispatcher_ids = ["control:{}".format(args.name)]

    for dispatcher_id in dispatcher_ids:
        if redis_store.exists(dispatcher_id):
            redis_store.hset(dispatcher_id, 'stop_scheduled', 'True')
            print "Stopping dispatcher %s" % redis_store.hget(dispatcher_id, 'name')


def control_scale(args):
    redis_store = args.redis_store
    dispatcher_id = "control:{}".format(args.name)
    if not redis_store.exists(dispatcher_id):
        print "Invalid dispatcher %s" % args.name
        sys.exit(1)

    print "Setting dispatcher {a.name} max_jobs to {a.jobs}".format(a=args)
    redis_store.hset(dispatcher_id, 'max_jobs', args.jobs)
