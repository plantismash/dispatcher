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
import argparse
import redis
from dispatcher.models import Notice
from datetime import datetime, timedelta

def parsedate(datestring):
    try:
        return datetime.strptime(datestring, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        pass
    try:
        return datetime.strptime(datestring, "%Y-%m-%d")
    except ValueError:
        pass
    try:
        return datetime.strptime(datestring, "%H:%M:%S")
    except ValueError:
        raise argparse.ArgumentTypeError('%r can not be parsed as a date' % datestring)


def setup_notice_options(subparsers):
    now = datetime.utcnow()
    next_week = now + timedelta(days=7)
    p_notice = subparsers.add_parser('notice',
                                      help="List, add, remove and change notices")
    notice_subparsers = p_notice.add_subparsers(title='notice-related commands')

    p_notice_list = notice_subparsers.add_parser('list',
                                                 help="List notices")
    p_notice_list.set_defaults(func=notice_list)

    p_notice_add = notice_subparsers.add_parser('add',
                                                help='Add a notice')
    p_notice_add.add_argument('teaser',
                              help='notice teaser')
    p_notice_add.add_argument('text',
                              help='notice text')
    p_notice_add.add_argument('--category', dest='category',
                              default='info', choices=['info', 'warning', 'error'],
                              help='Category of the notice')
    p_notice_add.add_argument('--show-from', dest='show_from',
                              default=now, type=parsedate,
                              help="Time to start showing the notice in YYYY-MM-DD HH:MM:SS format")
    p_notice_add.add_argument('--show-until', dest='show_until',
                              default=next_week, type=parsedate,
                              help="Time to start showing the notice in YYYY-MM-DD HH:MM:SS format")
    p_notice_add.set_defaults(func=notice_add)

    p_notice_remove = notice_subparsers.add_parser('remove',
                                                   help="Remove notices")
    p_notice_remove.add_argument("id",
                                 help="ID of notice to remove")
    p_notice_remove.set_defaults(func=notice_remove)


def notice_list(args):
    redis_store = redis.Redis.from_url(args.queue)
    notice_ids = redis_store.keys("notice:*")
    for notice_id in notice_ids:
        notice = redis_store.hgetall(notice_id)
        print """%(id)s
        %(category)s
        %(show_from)s
        %(show_until)s
    %(teaser)s
    %(text)s"""% notice


def notice_add(args):
    redis_store = redis.Redis.from_url(args.queue)
    notice = Notice(args.teaser, args.text, None, args.show_from,
                    args.show_until, args.category)
    redis_store.hmset("notice:{}".format(notice.id), notice.json)

def notice_remove(args):
    redis_store = redis.Redis.from_url(args.queue)
    if args.id == "all":
        notice_ids = redis_store.keys("notice:*")
    else:
        notice_ids = ["notice:{}".format(args.id)]

    for notice_id in notice_ids:
        if redis_store.exists(notice_id):
            print "Removing notice %r" % redis_store.hget(notice_id, 'teaser')
            redis_store.delete(notice_id)
    return
