# This file is part of antiSMASH and distributed under the same license
'''Unify storage access for all scripts'''

from urlparse import urlparse
import redis
from redis.sentinel import Sentinel


class AntismashStorageError(RuntimeError):
    '''Thrown on errors accessing the storage'''
    pass


def get_storage(queue, timeout=0.1):
    '''Open a storage connection given a redis or sentinel URI'''
    if queue.startswith('redis://'):
        redis_store = redis.Redis.from_url(queue)
    elif queue.startswith('sentinel://'):
        parsed_url = urlparse(queue)
        service = parsed_url.path.lstrip('/')
        port = 26379
        if ':' in parsed_url.netloc:
            host, str_port = parsed_url.netloc.split(':')
            port = int(str_port)
        else:
            host = parsed_url.netloc
        sentinel = Sentinel([(host, port)], socket_timeout=timeout)
        redis_store = sentinel.master_for(service, redis_class=redis.Redis, socket_timeout=timeout)
    else:
        raise AntismashStorageError('Unknown storage scheme {!r}'.format(queue))

    return redis_store
