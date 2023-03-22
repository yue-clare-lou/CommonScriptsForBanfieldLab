#!/usr/bin/env python

'''
This script is for submitting jobs to run on Banfield Lab's cluster nodes.
'''

import os
import argparse
import subprocess
import sys
import time

def get_job_commands(filename):
    commands = []
    with open(filename) as f:
        for line in f:
            commands.append(line.strip())
    return commands


def job_count(user_name):
    proc = subprocess.Popen(f'squeue | grep "{user_name}" | wc -l', shell=True, stdout=subprocess.PIPE)
    output = proc.communicate()[0]
    return int(output)


def run(filename, max_jobs, user_name):
    job_commands = get_job_commands(filename)
    current_job_count = job_count(user_name)
    num_jobs_submitted = 0
    while True:
        if len(job_commands) == 0:
            break
        if current_job_count < max_jobs:
            next_job = job_commands.pop(0)
            os.system(next_job)
            num_jobs_submitted += 1
            print(next_job)
            print(f'submitted {num_jobs_submitted} jobs')
        current_job_count = job_count(user_name)
        time.sleep(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sending jobs to queue in a controlled way.')
    parser.add_argument('-f', '--filename', type=str, required=True,
                        help='Command line file containing job commands to send to the clusters')
    parser.add_argument('-j', '--max-jobs', type=int, default=30,
                        help='Maximum number of jobs on the queue (default: 30)')
    parser.add_argument('-n', '--user-name', type=str, required=True,
                        help='Name of the user to search for in the job queue')
    args = parser.parse_args()

    run(args.filename, args.max_jobs, args.user_name)
