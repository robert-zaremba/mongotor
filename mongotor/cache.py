"""This work is based on implementation from pymongo.mongo_client
"""

import datetime

class CacheMixin(object):
    def __init__(self):
        self.__index_cache = {}

    def _cached(self, coll, index):
        """Test if `index` is cached.
        """
        key = '%s:%s' % (coll, index)
        cache = self.__index_cache
        return (key in cache and
                cache[key] > datetime.datetime.utcnow())

    def _cache_index(self, collection, index, cache_for=300):
        """Add an index to the index cache for ensure_index operations.
        """
        key = '%s:%s' % (collection, index)
        now = datetime.datetime.utcnow()
        expire = datetime.timedelta(seconds=cache_for) + now

        if key not in self.__index_cache:
            self.__index_cache[key] = expire
        else:
            self.__index_cache[key] = expire

    def _purge_index(self, collection=None, index=None):
        """Purge an index from the index cache.
        """
        if collection is None:
            self.__index_cache = {}
        else:
            key = '%s:%s' % (collection, index)
            if key in self.__index_cache:
                del self.__index_cache[key]
