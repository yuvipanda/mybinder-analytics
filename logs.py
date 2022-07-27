#!/usr/bin/env python3
import time
import argparse
import subprocess
import json
from dateutil.parser import isoparse
import sys

def get_job(name):
    cmd = [
        'gcloud', 'dataflow', 'jobs', 'list', f'--filter=name={name}', '--format=json'
    ]
    output = subprocess.check_output(cmd, encoding='utf-8')
    jobs = json.loads(output)
    if jobs:
        return jobs[0]
    return None


def get_job_logs(project_id, job_id, log_type_filter, since=None):
    query = [
        f'resource.labels.job_id="{job_id}"'
    ]
    if log_type_filter:
        query.append(f'logName="projects/{project_id}/logs/dataflow.googleapis.com%2F{log_type_filter}"')
    if since:
        query.append(f'timestamp>"{since}"')
    cmd = ["gcloud", "logging", "read", '\n'.join(query), '--format=json', '--order=asc']
    logs = json.loads(subprocess.check_output(cmd))
    for l in logs:
        ts = isoparse(l['timestamp'])
        print(ts, end=' ')

        # logType looks like projects/<project-id>/logs/dataflow.googleapis.com%2F<type>
        # And type is what we ultimately care about
        log_type = l["logName"].rsplit("%2F", 1)[-1]
        print(f"[{log_type}] ", end='')

        # Each log type should be handled differently
        if log_type in ('kubelet', 'shuffler', 'harness', 'harness-startup', 'vm-health', 'vm-monitor', 'resource', 'agent') :
            message = l['jsonPayload']['message']
        elif log_type in ('docker', 'system', 'shuffler-startup'):
            message = l['jsonPayload']['message']
        elif log_type in ('job-message'):
            message = l['textPayload']
        elif log_type in ('worker'):
            payload = l['jsonPayload']
            message = f'{payload["message"]}'
        elif log_type in ('insights',):
            # Let's ignore these
            continue
        else:
            print(log_type)
            print(l)
            sys.exit(1)
        if l['labels'].get('compute.googleapis.com/resource_type') == 'instance':
            worker = l['labels']['compute.googleapis.com/resource_name']
            message = f'[node:{worker}] {message}'
        # Trim additional newlines to prevent excess blank lines
        print(message.rstrip())

    # Return timestamp of last messag
    if logs:
        return logs[-1]['timestamp']
    else:
        return since

def main():
    argparser = argparse.ArgumentParser()

    argparser.add_argument('name')

    argparser.add_argument('--type')
    argparser.add_argument('--follow', '-f', action='store_true')

    args = argparser.parse_args()

    project = subprocess.check_output(['gcloud', 'config', 'get', 'project'], encoding='utf-8').strip()
    job = get_job(args.name)
    last_ts = None

    while True:
        last_ts = get_job_logs(project, job['id'], args.type, last_ts)
        if not args.follow:
            break
        time.sleep(5)


main()