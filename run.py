import apache_beam as beam
import apache_beam.io.fileio as fileio
from apache_beam.options.pipeline_options import PipelineOptions
import sys

import requests
import json
import datetime
import random

def yield_jsonl(data):
    # FIND OUT: Am I re-serializing data a lot by parsing this JSON here?
    return [json.loads(l) for l in data.splitlines()]

class AddLaunchTimestamp(beam.DoFn):
    """
    Tell Beam the timestamp of each event
    """
    def process(self, launch):
        ts = datetime.datetime.fromisoformat(launch["timestamp"]).timestamp()
        yield beam.window.TimestampedValue(launch, ts)
with beam.Pipeline(options=PipelineOptions(save_main_session=True)) as pipeline:
    # I must use beam.Create somewhere here apparently?
    launches = pipeline  \
        | 'FetchFilesList' >> fileio.MatchFiles('gs://binder-events-archive/events-*.jsonl') \
        | 'FetchAllFiles' >> fileio.ReadMatches() \
        | 'Reshuffle' >> beam.Reshuffle() \
        | 'ReadAllFiles' >> beam.Map(lambda x: x.read_utf8()) \
        | "FindLines" >> beam.FlatMap(yield_jsonl) \
        | "AddTimestamps" >> beam.ParDo(AddLaunchTimestamp())

    # launches | beam.Map(print)

    origin = launches \
        | "GroupByVersion" >> beam.GroupBy(lambda l: l.get("origin", "gke.mybinder.org")).aggregate_field(
            lambda x: 1, sum, "total"
        ) \
        | "Top N" >> beam.combiners.Top.Largest(10) \
        | beam.io.WriteToText("gs://dataflow-staging-us-central1-6c87af18aaf9bbb6114e862703086487/total-launches")
