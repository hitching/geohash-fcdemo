
import logging
from google.appengine.api import apiproxy_stub_map
from google.appengine.api import memcache
from google.pyglib.gexcept import AbstractMethod
from google.appengine.api import urlfetch as urlfetch_builtin
from google.appengine.api.datastore import datastore_pb, Query, MultiQuery
from google.appengine.ext.db import GqlQuery
#from asynctools import datastore
import datastore

class RpcTask(object):

    def __init__(self, rpc, *args, **kwargs):
        self.__user_rpc=rpc
        self.__args = args
        self.__kwargs = kwargs
        self.__cache_result = None
        self.__client_state = kwargs.get('client_state')

    def __set_runner(self, runner):
        self.__user_rpc.runner = runner
    def __get_runner(self):
        return self.__user_rpc.runner
    runner = property(__get_runner, __set_runner)

    @property
    def client_state(self):
        return self.__client_state

    @property
    def rpc(self):
        return self.__user_rpc

    @property
    def cache_key(self):
        raise AbstractMethod

    def __get_cache_result(self):
        return self.__cache_result

    def __set_cache_result(self, result):
        self.__cache_result = result

    cache_result = property(__get_cache_result, __set_cache_result)

    def make_call(self):
        """ common call to dispatch rpc
            access args and kwargs to call services make_call with arguments
        """
        raise AbstractMethod

    def wait(self):
        self.rpc.wait()

    def get_result(self):
        if self.cache_result is not None:
            return self.cache_result
        else:
            return self.rpc.get_result()

    @property
    def args(self):
        return self.__args

    @property
    def kwargs(self):
        return self.__kwargs

    def __repr__(self):
        try:
            return "%s%s" % (type(self), self.cache_key)
        except Exception: # cache_key may raise exception for tasks that cannot be cached
            return "%s %s %s" % (type(self), repr(self.__args), repr(self.__kwargs))


class UrlFetchTask(RpcTask):

    def __init__(self, url, deadline=None, callback=None, urlfetch=urlfetch_builtin, **kw):
        assert url, "Url cannot be None or empty string"
        self._fetch_mechanism = urlfetch
        rpc = self._fetch_mechanism.create_rpc(deadline=deadline, callback=callback)
        super(UrlFetchTask, self).__init__(rpc, url, **kw)
        self.__url = url

    @property
    def url(self):
        return self.__url

    @property
    def cache_key(self):
        """ compute cache key
            Throws attributeError if make_call_args has not been called
        """
        return self.__url

    def make_call(self):
        self._fetch_mechanism.make_fetch_call(self.rpc, *self.args, **self.kwargs)


class QueryTask(RpcTask):

    def __init__(self, query, limit=None, offset=None, deadline=None, callback=None, **kw):

        self.__query = query._get_query() if isinstance(query, GqlQuery) else query
        
        assert isinstance(self.__query, Query) and not isinstance(self.__query, MultiQuery), "Query must be of instance Query, MultiQuery not handled yet"

        self.__entities = []
        self.__exception = []
        self.__limit = limit
        self.__offset = offset
        self.__cache_key = "query=%s,limit=%s,offset=%s" % (str(self.__query),str(limit),str(offset))

        rpc = datastore.create_rpc(deadline=deadline, callback=callback)
        rpc.callback = lambda: datastore.run_callback(rpc, self.entities, self.exception, callback=callback)
        super(QueryTask, self).__init__(rpc, **kw)

    def get_result(self):
        if self.cache_result is not None:
            return self.cache_result
        if len(self.exception) >= 1:
            raise self.exception[0]
        else:
            return self.entities

    @property
    def cache_key(self):
        return self.__cache_key

    def make_call(self):
        pb = self.__query._ToPb(self.limit, self.offset)
        result = datastore_pb.QueryResult()
        datastore.make_run_call(self.rpc, pb, result)

    @property
    def limit(self):
        return self.__limit

    @property
    def offset(self):
        return self.__offset

    @property
    def query(self):
        return self.__query

    @property
    def entities(self):
        return self.__entities

    @property
    def exception(self):
        return self.__exception


class AsyncMultiTask(list):
    """
        Context for running async tasks in.
        Add an rpc that is ready to be Waited on.
        After it has been run it should be ready to have CheckSuccess called.
    """
    def __init__(self, tasks=None):
        if tasks is None:
            super(AsyncMultiTask, self).__init__()
        else:
            super(AsyncMultiTask, self).__init__(tasks)

        for task in self:
            task.runner = self

    def run(self):
        """Runs the tasks, some tasks create additional rpc objects which are appended to self
           when all tasks and rpcs have been waited on the extra items are deleted from self
        """
        tasks = list(self)
        [ task.make_call() for task in self ]
        [ task.wait() for task in self ]
        self[:] = tasks

    def append(self, task):
        """Bind self to the task so the task, userrpc can append additional tasks to be run"""
        list.append(self, task)
        if isinstance(task, (RpcTask, apiproxy_stub_map.UserRPC)):
            task.runner = self

    def __repr__(self):
        return "%s%s" % (type(self), list.__repr__(self))


def determine_cache_hits_misses(tasks, cache_results):
    have = []
    todo = []
    for task in tasks:
        result = cache_results.get(task.cache_key)
        if result:
            have.append(task)
            task.cache_result = result
        else:
            todo.append(task)
    return (have, todo)


class CachedMultiTask(list):
    def __init__(self, tasks=None, time=0, namespace=None, memcache=memcache.Client(), runner_type=AsyncMultiTask):
        if tasks is None:
            super(CachedMultiTask,self).__init__()
        else:
            super(CachedMultiTask,self).__init__(tasks)
        self.time = time
        self.namespace = namespace
        self.memcache = memcache
        self.runner_type = runner_type

    def run(self):
        """
        run tasks asyncronously, tasks may create additional UserRPC objects that are also inturn waited on.

        1. Fetch from memcache
        2. Filter into hits and misses (have, todo)
        3. Async run todo
        4. Set todo results into memcache
        """
        cache_keys = [t.cache_key for t in self]

        cache_results = self.memcache.get_multi(cache_keys, namespace=self.namespace)

        have, todo = determine_cache_hits_misses(self, cache_results)
        # determine cached tasks
        if len(todo) > 0:
            task_runner = self.runner_type(todo)
            task_runner.run()
        set_dict = {}
        for task in todo:
            try:
                set_dict[task.cache_key] = task.get_result()
            except Exception:
                logging.info("Exception retrieving items after cache miss. Continuing.", exc_info=True)
        if set_dict:
            failed = self.memcache.set_multi(set_dict, time=self.time, namespace=self.namespace)
            if failed:
                logging.info("Memcache set_multi failed. %d items failed: %s" (len(failed), failed))
            if len(failed) == len(todo):
                logging.error("Memcache set_multi failed entirely.")

    def __repr__(self):
        return "%s%s" % (type(self), list.__repr__(self))
