#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A workflow emitting the top k most common words for each prefix."""

from __future__ import absolute_import

import argparse
import logging
import re
import uuid

from google.cloud.proto.datastore.v1 import entity_pb2
from google.cloud.proto.datastore.v1 import query_pb2
from googledatastore import helper as datastore_helper, PropertyFilter
#from google.cloud import datastore


import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
from apache_beam.io.gcp.datastore.v1.datastoreio import WriteToDatastore
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None):

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      required=True,
                      help='Input file to process.')
  parser.add_argument('--output',
                      required=False,
                      help='Output file to write results to.')
  parser.add_argument('--project',
                      required=False,
                      help='Project ID for datastore')
  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:

    results = (p  # pylint: disable=expression-not-assigned
     | 'read' >> ReadFromText(known_args.input)
     | 'split' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
     | 'TopPerPrefix' >> TopPerPrefix(5, "")
    # | 'format' >> beam.Map(
    #     lambda (prefix, candidates): '%s: %s' % (prefix, candidates))
     | 'create entity' >> beam.Map(
         lambda (prefix, candidates): EntityWrapper().make_entity(prefix, candidates))
     | 'write to datastore' >> WriteToDatastore(known_args.project)
  #   | 'write' >> WriteToText(known_args.output)
    )
  

class EntityWrapper(object):
  """Create a Cloud Datastore entity from the given string."""
  def make_entity(self, prefix, candidates):
    entity = entity_pb2.Entity()
    datastore_helper.add_key_path(entity.key, "prefix", prefix,
                                  "id", str(uuid.uuid4()), "candidates", unicode(candidates))

    # All entities created will have the same ancestor
    #datastore_helper.add_properties(entity, {"wordlist": unicode(candidates)})
    #datastore_helper.add_properties(entity, {"candidates": unicode(candidates)})
    return entity




class TopPerPrefix(beam.PTransform):

  def __init__(self, count, project):
    super(TopPerPrefix, self).__init__()
    self._count = count
    self._project = project

  def expand(self, words):
    """Compute the most common words for each possible prefixes.
    Args:
      words: a PCollection of strings
    Returns:
      A PCollection of most common words with each prefix, in the form
          (prefix, [(count, word), (count, word), ...])
    """
    return (words
            | beam.combiners.Count.PerElement()
            | beam.FlatMap(extract_prefixes)
            | beam.combiners.Top.LargestPerKey(self._count))


def extract_prefixes(element):
  word, count = element
  for k in range(1, len(word) + 1):
    prefix = word[:k]
    yield prefix, (count, word)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()