import apache_beam as beam
import apache_beam.io.fileio as fileio
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import sys
import io

import requests
import json
import datetime
import typing
import random

class JSONCoder(beam.coders.Coder):
    def decode(self, encoded):
        return json.loads(encoded)

    def encode(self, value: typing.Any) -> bytes:
        return json.dumps(value).encode()

class CSVCoder(beam.coders.Coder):
    def encode(self, value: typing.Any) -> bytes:
        with io.StringIO() as f:
            # This is a little excessive, but what else am I gonna do - ",".join!?
            csv.writer(f).writerow(value)
            return f.getvalue().encode()


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
        | 'FetchAllFiles' >> beam.io.ReadFromText('gs://binder-events-archive/events-*.jsonl', coder=JSONCoder())

    origin = launches \
        | "GroupByOrigin" >> beam.GroupBy(lambda l: l.get("origin", "gke.mybinder.org")).aggregate_field(
            lambda x: 1, sum, "total"
        ) \
        | beam.io.WriteToText("gs://dataflow-staging-us-central1-6c87af18aaf9bbb6114e862703086487/origin", coder=CSVCoder())

    provider = launches \
        | "GroupByProvider" >> beam.GroupBy(lambda l: l.get("provider")).aggregate_field(
            lambda x: 1, sum, "total"
        ) \
        | "WriteProvider" >> beam.io.WriteToText("gs://dataflow-staging-us-central1-6c87af18aaf9bbb6114e862703086487/provider", coder=CSVCoder())


    specs = launches \
        | "GroupBySpec" >> beam.GroupBy(lambda l: l.get("spec")).aggregate_field(
            lambda x: 1, sum, "total"
        ) \
        | "WriteSpec" >> beam.io.WriteToText("gs://dataflow-staging-us-central1-6c87af18aaf9bbb6114e862703086487/spec", coder=CSVCoder())