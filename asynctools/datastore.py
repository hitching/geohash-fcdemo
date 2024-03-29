#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""The Python datastore API used by app developers.

Defines Entity, Query, and Iterator classes, as well as methods for all of the
datastore's calls. Also defines conversions between the Python classes and
their PB counterparts.

The datastore errors are defined in the datastore_errors module. That module is
only required to avoid circular imports. datastore imports datastore_types,
which needs BadValueError, so it can't be defined in datastore.
"""
import logging

from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_errors
from google.appengine.api.datastore_types import Key
from google.appengine.datastore import datastore_index
from google.appengine.datastore import datastore_pb
from google.appengine.runtime import apiproxy_errors

from google.appengine.api.datastore import _ToDatastoreError
from google.appengine.api.datastore import Entity




def make_run_call(rpc, request, response):
    """Runs this query, with an optional result limit and an optional offset.

    Identical to Run, with the extra optional limit and offset parameters.
    limit and offset must both be integers >= 0.

    This is not intended to be used by application developers. Use Get()
    instead!
    """
    rpc.make_call('RunQuery', request, response)

def run_rpc_handler(rpc):
  try:
    rpc.check_success()
  except apiproxy_errors.ApplicationError, err:
    try:
      _ToDatastoreError(err)
    except datastore_errors.NeedIndexError, exc:
      yaml = datastore_index.IndexYamlForQuery(
        *datastore_index.CompositeIndexForQuery(rpc.request)[1:-1])
      raise datastore_errors.NeedIndexError(
        str(exc) + '\nThis query needs this index:\n' + yaml)

  return rpc.response

def process_query_result(result):
    if result.keys_only():
        return [Key._FromPb(e.key()) for e in result.result_list()]
    else:
        return [Entity._FromPb(e) for e in result.result_list()]


def run_callback(rpc, entities, exception, callback=None):
    try:
        assert isinstance(rpc.request,datastore_pb.Query), "request should be a query"
        assert isinstance(rpc.response,datastore_pb.QueryResult), "response should be a QueryResult"

        response = run_rpc_handler(rpc)
        entities += process_query_result(response)
        limit = rpc.request.limit()

        if len(entities) > limit:
            del entities[limit:]
        elif response.more_results() and len(entities) < limit:
            # create rpc for running


            count = limit - len(entities)

            req = datastore_pb.NextRequest()
            req.set_count(count)
            req.mutable_cursor().CopyFrom(rpc.response.cursor())
            result = datastore_pb.QueryResult()

            nextrpc = create_rpc(deadline=rpc.deadline)
            nextrpc.callback = lambda: next_callback(nextrpc, entities, exception, callback=callback)
            rpc.runner.append(nextrpc)

            nextrpc.make_call('Next', req, result)

            if rpc.runner:
                rpc.runner.append(nextrpc)
            else:
                nextrpc.Wait()

    except (datastore_errors.Error, apiproxy_errors.Error), exp:
        logging.debug("Exception (RunQuery):"+str(exp))
        exception.append(exp)
        if callback:
            callback(rpc)

def next_rpc_handler(rpc):
    try:
        rpc.check_success()
    except apiproxy_errors.ApplicationError, err:
      logging.debug("next_rpc_handler")
      raise _ToDatastoreError(err)
    return rpc.response

def next_callback(rpc, entities, exception, callback=None):
    try:
        assert isinstance(rpc.request,datastore_pb.NextRequest), "request should be a query"
        assert isinstance(rpc.response,datastore_pb.QueryResult), "response should be a QueryResult"

        result = next_rpc_handler(rpc)
        entity_list = process_query_result(result)
        count = rpc.request.count()

        if len(entity_list) > count:
            del entity_list[count:]

        entities += entity_list


        if result.more_results() and len(entity_list) < count:
            # create rpc for running


            count = count - len(entity_list)

            req = datastore_pb.NextRequest()
            req.set_count(count)
            req.mutable_cursor().CopyFrom(rpc.response.cursor())
            result = datastore_pb.QueryResult()

            nextrpc = create_rpc(deadline=rpc.deadline)
            nextrpc.callback = lambda: next_callback(nextrpc, entities, exception, callback=callback)
            rpc.runner.append(nextrpc)

            nextrpc.MakeCall('Next', req, result)

            if rpc.runner:
                rpc.runner.append(nextrpc)
            else:
                nextrpc.Wait()

    except (datastore_errors.Error, apiproxy_errors.Error), exp:
        logging.debug("Exception (Next):"+str(exp))
        exception.append(exp)
        if callback:
            callback(rpc)



def create_rpc(deadline=None, callback=None):
    return apiproxy_stub_map.UserRPC('datastore_v3', deadline, callback)






