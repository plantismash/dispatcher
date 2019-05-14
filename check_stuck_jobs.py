#!/usr/bin/env python
#
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
"""Check for stuck jobs
"""
from __future__ import print_function
import os
from os import path
import redis
from optparse import OptionParser
from datetime import datetime, timedelta
from dispatcher.models import Job
from pprint import pprint

version = "%prog 0.0.2"

def main():
    """Parse the command line, set up the database, start the main loop"""
    queue_default = os.getenv('AS_QUEUE', "redis://localhost:6379/0")
    parser = OptionParser(version=version)
    parser.add_option('-q', '--queue', dest="queue",
                      help="URI of the database containing the job queue (%s)" % queue_default,
                      default=queue_default)
    parser.add_option('--duration', dest="duration",
                      type=int, default=2,
                      help="How many days a job can go without an update before being listed (default: 2)")
    parser.add_option('--pretty', dest="pretty",
                      action="store_true", default=False,
                      help="Prettify the output")
    (options, args) = parser.parse_args()

    redis_store = redis.Redis.from_url(options.queue)

    running_jobs = redis_store.lrange("jobs:running", 0, -1)

    stuck_jobs = []

    delta = timedelta(days=options.duration)

    for j in running_jobs:
        job = Job(**redis_store.hgetall("job:%s" % j))
        today = datetime.utcnow()
        if job.last_changed < today - delta:
            stuck_jobs.append(job)


    stuck_count = len(stuck_jobs)
    if stuck_count > 0:
        print("{0} jobs stuck for {1} days or more:".format(
              stuck_count, options.duration))
        for s in stuck_jobs:
            if options.pretty:
                print("{uid} on {dispatcher} stuck since {last_changed} with status {status}".format(**s.get_dict()))
            else:
                pprint(s.get_dict())


if __name__ == "__main__":
    main()
