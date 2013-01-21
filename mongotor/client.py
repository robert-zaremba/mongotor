# coding: utf-8
# <mongotor - An asynchronous driver and toolkit for accessing MongoDB with Tornado>
# Copyright (C) <2012>  Marcel Nicolay <marcel.nicolay@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import logging
from bson.code import Code
from tornado import gen
from mongotor.node import ReadPreference
from mongotor.cursor import Cursor
from mongotor import message
from mongotor import helpers

from pymongo.helpers import _index_list, _index_document

log = logging.getLogger(__name__)


class Client(object):

    def __init__(self, database, collection):
        self._database = database
        self._collection = collection
        self._collection_name = database.get_collection_name(collection)

    def insert(self, doc_or_docs, safe=True, check_keys=True, callback=None):
        """Insert a document

        :Parameters:
          - `doc_or_docs`: a document or list of documents to be
            inserted
          - `safe` (optional): check that the insert succeeded?
          - `check_keys` (optional): check if keys start with '$' or
            contain '.', raising :class:`~pymongo.errors.InvalidName`
            in either case
          - `callback` : method which will be called when save is finished
        """
        if isinstance(doc_or_docs, dict):
            doc_or_docs = [doc_or_docs]

        assert isinstance(doc_or_docs, list)

        msg = message.insert(self._collection_name, doc_or_docs,
                             check_keys, safe, {})
        log.debug("mongo: db.{0}.insert({1})".format(self._collection_name, doc_or_docs))
        node = self._database.get_node(ReadPreference.PRIMARY)
        node.connection(lambda conn: conn.send_message(msg, safe, False, callback))

    def remove(self, spec_or_id={}, safe=True, callback=None):
        """remove a document

        :Parameters:
        - `spec_or_id`: a query or a document id
        - `safe` (optional): safe insert operation
        - `callback` : method which will be called when save is finished
        """
        if not isinstance(spec_or_id, dict):
            spec_or_id = {"_id": spec_or_id}

        assert isinstance(spec_or_id, dict)

        msg = message.delete(self._collection_name, spec_or_id,
                             safe, {})
        log.debug("mongo: db.{0}.remove({1})".format(self._collection_name, spec_or_id))
        node = self._database.get_node(ReadPreference.PRIMARY)
        node.connection(lambda conn: conn.send_message(msg, safe, False, callback))

    def update(self, spec, document, upsert=False, safe=True,
        multi=False, callback=None):
        """Update a document(s) in this collection.

        :Parameters:
          - `spec`: a ``dict`` or :class:`~bson.son.SON` instance
            specifying elements which must be present for a document
            to be updated
          - `document`: a ``dict`` or :class:`~bson.son.SON`
            instance specifying the document to be used for the update
            or (in the case of an upsert) insert - see docs on MongoDB
            `update modifiers`_
          - `upsert` (optional): perform an upsert if ``True``
          - `safe` (optional): check that the update succeeded?
          - `multi` (optional): update all documents that match
            `spec`, rather than just the first matching document. The
            default value for `multi` is currently ``False``, but this
            might eventually change to ``True``. It is recommended
            that you specify this argument explicitly for all update
            operations in order to prepare your code for that change.
        """
        if not isinstance(spec, dict):
            spec = {'_id': spec}
        assert isinstance(document, dict), "document must be an instance of dict"
        assert isinstance(upsert, bool), "upsert must be an instance of bool"
        assert isinstance(safe, bool), "safe must be an instance of bool"

        msg = message.update(self._collection_name, upsert,
                             multi, spec, document, safe, {})
        log.debug("mongo: db.{0}.update({1}, {2}, {3}, {4})".format(
            self._collection_name, spec, document, upsert, multi))

        node = self._database.get_node(ReadPreference.PRIMARY)
        node.connection(lambda conn: conn.send_message(msg, safe, False, callback))

    def find_one(self, spec_or_id=None, **kwargs):
        """Get a single document from the database.

        All arguments to :meth:`find` are also valid arguments for
        :meth:`find_one`, although any `limit` argument will be
        ignored. Returns a single document, or ``None`` if no matching
        document is found.

        :Parameters:

          - `spec_or_id` (optional): a dictionary specifying
            the query to be performed OR any other type to be used as
            the value for a query for ``"_id"``.

          - `**kwargs` (optional): any additional keyword arguments
            are the same as the arguments to :meth:`find`.
        """
        if spec_or_id is not None and not isinstance(spec_or_id, dict):
            spec_or_id = {"_id": spec_or_id}

        self.find(spec_or_id, limit=-1, **kwargs)

    def find(self, *args, **kwargs):
        """Query the database.

        Returns `Cursor` object unless callback is specified.
        Otherwise it fetch all results (through `Cursor.find` method) and
        sends them to callback.

        The `spec` argument is a prototype document that all results
        must match.

        :Parameters:
          - `spec` (optional): a SON object specifying elements which
            must be present for a document to be included in the
            result set
          - `fields` (optional): a list of field names that should be
            returned in the result set ("_id" will always be
            included), or a dict specifying the fields to return
          - `skip` (optional): the number of documents to omit (from
            the start of the result set) when returning the results
          - `limit` (optional): the maximum number of results to
            return
          - `timeout` (optional): if True, any returned cursor will be
            subject to the normal timeout behavior of the mongod
            process. Otherwise, the returned cursor will never timeout
            at the server. Care should be taken to ensure that cursors
            with timeout turned off are properly closed.
          - `snapshot` (optional): if True, snapshot mode will be used
            for this query. Snapshot mode assures no duplicates are
            returned, or objects missed, which were present at both
            the start and end of the query's execution. For details,
            see the `snapshot documentation
            <http://dochub.mongodb.org/core/snapshot>`_.
          - `tailable` (optional): the result of this find call will
            be a tailable cursor - tailable cursors aren't closed when
            the last data is retrieved but are kept open and the
            cursors location marks the final document's position. if
            more data is received iteration of the cursor will
            continue from the last document received. For details, see
            the `tailable cursor documentation
            <http://www.mongodb.org/display/DOCS/Tailable+Cursors>`_.
          - `sort` (optional): a list of (key, direction) pairs
            specifying the sort order for this query. See
            :meth:`~pymongo.cursor.Cursor.sort` for details.
          - `max_scan` (optional): limit the number of documents
            examined when performing the query
          - `read_preferences` (optional): The read preference for
            this query.
        """

        log.debug("mongo: db.{0}.find({spec}).limit({limit}).sort({sort})".format(
            self._collection_name,
            spec=args[0] if args else {},
            sort=kwargs.get('sort', {}),
            limit=kwargs.get('limit', '')
        ))
        cursor = Cursor(self._database, self._collection,
            *args, **kwargs)

        if 'callback' in kwargs:
            cursor.find(callback=kwargs['callback'])
        else:
            return cursor

    def distinct(self, key, callback):
        """Get a list of distinct values for `key` among all documents
        in this collection.

        Raises :class:`TypeError` if `key` is not an instance of
        :class:`basestring` (:class:`str` in python 3).

        To get the distinct values for a key in the result set of a
        query use :meth:`~mongotor.cursor.Cursor.distinct`.

        :Parameters:
          - `key`: name of key for which we want to get the distinct values

        """
        self.find().distinct(key, callback=callback)

    def count(self, callback):
        """Get the size of the results among all documents.

        Returns the number of documents in the results set
        """
        self.find().count(callback=callback)

    @gen.engine
    def aggregate(self, pipeline, read_preference=None, callback=None):
        """Perform an aggregation using the aggregation framework on this
        collection.

        :Parameters:
          - `pipeline`: a single command or list of aggregation commands
          - `read_preference`

        .. note:: Requires server version **>= 2.1.0**

        .. _aggregate command:
            http://docs.mongodb.org/manual/applications/aggregation
        """
        if not isinstance(pipeline, (dict, list, tuple)):
            raise TypeError("pipeline must be a dict, list or tuple")

        if isinstance(pipeline, dict):
            pipeline = [pipeline]

        response, error = yield gen.Task(self._database.command,
            "aggregate", self._collection, pipeline=pipeline,
            read_preference=read_preference)

        callback(response)

    @gen.engine
    def group(self, key, condition, initial, reduce, finalize=None,
        read_preference=None, callback=None):
        """Perform a query similar to an SQL *group by* operation.

        Returns an array of grouped items.

        The `key` parameter can be:

          - ``None`` to use the entire document as a key.
          - A :class:`list` of keys (each a :class:`basestring`
            (:class:`str` in python 3)) to group by.
          - A :class:`basestring` (:class:`str` in python 3), or
            :class:`~bson.code.Code` instance containing a JavaScript
            function to be applied to each document, returning the key
            to group by.

        :Parameters:
          - `key`: fields to group by (see above description)
          - `condition`: specification of rows to be
            considered (as a :meth:`find` query specification)
          - `initial`: initial value of the aggregation counter object
          - `reduce`: aggregation function as a JavaScript string
          - `finalize`: function to be called on each object in output list.

        """

        group = {}
        if isinstance(key, basestring):
            group["$keyf"] = Code(key)
        elif key is not None:
            group = {"key": helpers._fields_list_to_dict(key)}

        group["ns"] = self._collection
        group["$reduce"] = Code(reduce)
        group["cond"] = condition
        group["initial"] = initial
        if finalize is not None:
            group["finalize"] = Code(finalize)

        response, error = yield gen.Task(self._database.command,
            "group", group, read_preference=read_preference)

        callback(response)

    def create_index(self, key_or_list, cache_for=300, **kwargs):
        """Creates an index on this collection.

        Takes either a single key or a list of (key, direction) pairs.
        The key(s) must be an instance of :class:`basestring`
        (:class:`str` in python 3), and the directions must be one of
        (:data:`~pymongo.ASCENDING`, :data:`~pymongo.DESCENDING`,
        :data:`~pymongo.GEO2D`). Returns the name of the created index.

        All optional index creation paramaters should be passed as
        keyword arguments to this method. Valid options include:
        check http://docs.mongodb.org/manual/reference/method/db.collection.ensureIndex/#db.collection.ensureIndex
        """
        keys = _index_list(key_or_list)
        index = {"key": _index_document(keys), "ns": self._collection_name}
        name = "name" in kwargs and kwargs["name"] or helpers._gen_index_name(keys)
        index["name"] = name
        index.update(kwargs)

        Client(self._database, 'system.indexes').insert(index, check_keys=False)
        self._database._cache_index(self._collection, name, cache_for)

        return name

    def ensure_index(self, key_or_list, cache_for=300, **kwargs):
        """Ensures that an index exists on this collection.

        Unlike :meth:`create_index`, which attempts to create an index
        unconditionally, :meth:`ensure_index` takes advantage of some
        caching within the driver such that it only attempts to create
        indexes that might not already exist. When an index is created
        (or ensured) by PyMongo it is "remembered" for `cache_for`
        seconds. Repeated calls to :meth:`ensure_index` within that
        time limit will be lightweight - they will not attempt to
        actually create the index.

        Care must be taken when the database is being accessed through
        multiple Database objects at once. If an index is created and
        then deleted using another database object any call to
        :meth:`ensure_index` within the cache window will fail to
        re-create the missing index.

        .. seealso:: :meth:`create_index`
        """
        if "name" in kwargs:
            name = kwargs["name"]
        else:
            keys = _index_list(key_or_list)
            name = kwargs["name"] = helpers._gen_index_name(keys)

        if not self._database._cached(self._collection, name):
            return self.create_index(key_or_list, cache_for, **kwargs)

    def drop_indexes(self):
        """Drops all indexes on this collection.

        Can be used on non-existant collections or collections with no indexes.
        Raises OperationFailure on an error.
        """
        self._database._purge_index()
        self.drop_index(u"*")

    def drop_index(self, name):
        """Drops the specified index on this collection.

        Can be used on non-existant collections or collections with no
        indexes.  Raises OperationFailure on an error. `name`
        can be either an index name (as returned by `create_index`),
        or an index specifier (as passed to `create_index`). An index
        specifier should be a list of (key, direction) pairs. Raises
        TypeError if index is not an instance of (str, unicode, list).

        .. warning::

          if a custom name was used on index creation (by
          passing the `name` parameter to :meth:`create_index` or
          :meth:`ensure_index`) the index **must** be dropped by name.

        :Parameters:
          - `name`: index (or name of index) to drop
        """
        if isinstance(name, list):
            name = helpers._gen_index_name(name)
        if not isinstance(name, basestring):
            raise TypeError("index_or_name must be an index name or list")

        self._database._purge_index(self._collection, name)
        self._database.command("dropIndexes", self._collection, index=name,
                                allowable_errors=["ns not found"])

    def reindex(self):
        """Rebuilds all indexes on this collection.

        .. warning:: reindex blocks all other operations (indexes
           are built in the foreground) and will be slow for large
           collections.
        """
        return self._database.command("reIndex", self._collection)
