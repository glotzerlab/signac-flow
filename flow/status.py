#!/usr/bin/env python
"""Attempt to determine the status of jobs in the workspace.

This module will use the local environment to determine the status of jobs."""

from __future__ import print_function
import os
import sys
import argparse
import multiprocessing
from collections import defaultdict
from itertools import islice

import signac
import flow
import util
from util.tqdm import tqdm

import jobs as jobdefs
import environment

ENV = flow.environment.get_environment()
project = signac.get_project()


def expand_bundled_jobs(project, scheduler_jobs):
    # Expand jobs which were submitted as part of a bundle
    for job in scheduler_jobs:
        if job.name().startswith('{}-bundle-'.format(project)):
            fn_bundle = os.path.join(project.root_directory(), job.name())
            with open(fn_bundle) as file:
                for line in file:
                    yield flow.manage.ClusterJob(line.strip(), job.status())
        else:
            yield job


def is_active(status):
    for gid, s in status.items():
        if s > flow.manage.JobStatus.inactive:
            return True
    return False


def fetch_job_status(args):
    project, job, scheduler_jobs = args
    result = dict()
    result['job_id'] = str(job)
    flow.manage.update_status(job, scheduler_jobs)
    status = job.document.get('status', dict())
    result['active'] = is_active(status)
    result['labels'] = sorted(set(jobdefs.classify(job)))
    if not result['labels']:
        return
    result['next_job'] = jobdefs.next_job(job)
    highest_status = max(status.values()) if len(status) else 1
    result['submission_status'] = [flow.manage.JobStatus(highest_status).name]
    return result


def format_row(status, statepoint=None):
    row = [
        status['job_id'],
        ', '.join(status['submission_status']),
        status['next_job'],
        ', '.join(status.get('labels', [])),
    ]
    if statepoint:
        sps = project.open_job(id=status['job_id']).statepoint()
        for i, sp in enumerate(statepoint):
            row.insert(i+1, sps.get(sp))
    return row


def draw_progressbar(value, total, width=40):
    n = int(value / total * width)
    return '|' + ''.join(['#'] * n) + ''.join(['-'] * (width - n)) + '|'


def overview(stati):
    # determine progress of all labels
    progress = defaultdict(int)
    for status in stati:
        for label in status['labels']:
            progress[label] += 1
    progress_sorted = sorted(
        progress.items(), key=lambda x: (x[1], x[0]), reverse=True)
    table_header = ['label', 'progress']
    rows = ([label, '{} {:0.2f}%'.format(
        draw_progressbar(num, len(stati)), 100 * num / len(stati))]
        for label, num in progress_sorted)
    print("Total # of jobs: {}".format(len(stati)))
    print(util.tabulate.tabulate(rows, headers=table_header))


def detailed_view(stati, statepoint=None):
    table_header = ['job_id', 'status', 'next_job', 'labels']
    if statepoint:
        for i,sp in enumerate(statepoint):
            table_header.insert(i+1, sp)
    rows = (format_row(status, statepoint)
            for status in stati
            if not (args.inactive and status['active']) and
            (status['active'] or status['next_job']))
    print(util.tabulate.tabulate(rows, headers=table_header))


def main(args):
    project = signac.get_project()
    try:
        scheduler = environment.get_scheduler(ENV)
        scheduler_jobs = {job.name(): job for job in
                          expand_bundled_jobs(project, scheduler.jobs())}
    except ImportError as error:
        print("WARNING: Requested scheduler not available. "
              "Unable to determine batch job status.", file=sys.stderr)
        print(error, file=sys.stderr)
        scheduler_jobs = None
    filter_ = None if args.filter is None else eval(args.filter)
    print("Finding all jobs...", file=sys.stderr)
    N = project.num_jobs()
    if args.cache:
        query = {'statepoint.states': {'$exists': True}}
        docs = signac.get_database('shape_potentials').index.find(query)
        jobs = [project.open_job(id=doc['signac_id'])
                for doc in tqdm(docs, total=docs.count())]
    else:
        jobs = [job for job in tqdm(islice(project.find_jobs(
            filter_), N), total=N) if 'states' in job.statepoint()]
        print("Determine job stati...", file=sys.stderr)
    with multiprocessing.Pool() as pool:
        stati = pool.imap_unordered(
            fetch_job_status, [(project, job, scheduler_jobs) for job in jobs])
        stati = [status for status in filter(
            None, tqdm(stati, total=len(jobs)))]
    print("Done.", file=sys.stderr)
    job_dict = {str(job): job for job in jobs}
    if args.export:
        mc = signac.get_database(str(project)).job_stati
        for status in stati:
            status['statepoint'] = job_dict[status['job_id']].statepoint()
            mc.update_one({'_id': status['job_id']}, {
                          '$set': status}, upsert=True)
    title = "Status project '{}':".format(project)
    print('\n' + title)
    overview(stati)
    if args.detailed_view:
        print()
        print("Detailed view:")
        detailed_view(stati, args.statepoint)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Display status of jobs.")
    parser.add_argument(
        '-f', '--filter',
        type=str,
        help="Filter jobs.")
    parser.add_argument(
        '-d', '--detailed-view',
        action='store_true',
        help="Display a detailed view of the job stati.")
    parser.add_argument(
        '-s', '--statepoint',
        type=str,
        nargs='*',
        help="Display a select parameter of the statepoint "
             "with the detailed view.")
    parser.add_argument(
        '-i', '--inactive',
        action='store_true',
        help="Display all jobs, that require attention.")
    parser.add_argument(
        '-e', '--export',
        action='store_true',
        help="Export job stati to MongoDB database collection.")
    parser.add_argument(
        '-c', '--cache',
        action='store_true',
        help="Use the cached index for loading jobs.")
    args = parser.parse_args()
    main(args)
