# TODO do we use all these?
import argparse
import contextlib
import functools
import logging
import os
import random
import re
import sys
import time
from datetime import datetime
from enum import Enum, auto
from subprocess import call

import attr
import click
import pendulum
import psutil

from plotman import chia
from plotman import madmax


def job_phases_for_tmpdir(d, all_jobs):
    '''Return phase 2-tuples for jobs running on tmpdir d'''
    return sorted([j.progress() for j in all_jobs if j.tmpdir == d])

def job_phases_for_dstdir(d, all_jobs):
    '''Return phase 2-tuples for jobs outputting to dstdir d'''
    return sorted([j.progress() for j in all_jobs if j.dstdir == d])

def is_plotting_cmdline(cmdline):
    return cmdline and 'chia_plot' in cmdline[0].lower()

def parse_chia_plot_time(s):
    # This will grow to try ISO8601 as well for when Chia logs that way
    return pendulum.from_format(s, 'ddd MMM DD HH:mm:ss YYYY', locale='en', tz=None)

def parse_chia_plots_create_command_line(command_line):
    command_line = list(command_line)

    if 'chia_plot' not in [arg.lower() for arg in command_line]:
        return ParsedChiaPlotsCreateCommand(error="not a madmax plotter process")
    # Parse command line args
    # if 'python' in command_line[0].lower():
    #     command_line = command_line[1:]
    # assert len(command_line) >= 3
    # assert 'chia' in command_line[0]
    # assert 'plots' == command_line[1]
    # assert 'create' == command_line[2]    

    # all_command_arguments = command_line[3:]
    all_command_arguments = command_line[1:]

    # nice idea, but this doesn't include -h
    help_option_names = {'--help', '-h'}

    command_arguments = [
        argument
        for argument in all_command_arguments
        if argument not in help_option_names
    ]

    # TODO: We could at some point do chia version detection and pick the
    #       associated command.  For now we'll just use the latest one we have
    #       copied.
    # command = chia.commands.latest_command()
    command = madmax.commands.latest_command()
    try:
        context = command.make_context(info_name='', args=list(command_arguments))
    except click.ClickException as e:
        error = e
        params = {}
    else:
        error = None
        params = context.params

    return ParsedChiaPlotsCreateCommand(
        error=error,
        help=len(all_command_arguments) > len(command_arguments),
        parameters=params,
    )

class ParsedChiaPlotsCreateCommand:
    def __init__(self, error, help=None, parameters=None):
        self.error = error
        self.help = help
        self.parameters = parameters

@functools.total_ordering
@attr.frozen(order=False)
class Phase:
    major: int = 0
    minor: int = 0
    known: bool = True

    def __lt__(self, other):
        return (
            (not self.known, self.major, self.minor)
            < (not other.known, other.major, other.minor)
        )

    @classmethod
    def from_tuple(cls, t):
        if len(t) != 2:
            raise Exception(f'phase must be created from 2-tuple: {t!r}')

        if None in t and not t[0] is t[1]:
            raise Exception(f'phase can not be partially known: {t!r}')

        if t[0] is None:
            return cls(known=False)

        return cls(major=t[0], minor=t[1])

    @classmethod
    def list_from_tuples(cls, l):
        return [cls.from_tuple(t) for t in l]

# TODO: be more principled and explicit about what we cache vs. what we look up
# dynamically from the logfile
class Job:
    'Represents a plotter job'

    logfile = ''
    jobfile = ''
    job_id = 0
    plot_id = '--------'
    proc = None   # will get a psutil.Process

    # These are dynamic, cached, and need to be udpated periodically
    phase = Phase(major=0, minor=0)   # Phase/subphase

    time_without_updates_in_min = 0

    def get_running_jobs(logroot, cached_jobs=()):
        '''Return a list of running plot jobs.  If a cache of preexisting jobs is provided,
           reuse those previous jobs without updating their information.  Always look for
           new jobs not already in the cache.'''
        jobs = []
        cached_jobs_by_pid = { j.proc.pid: j for j in cached_jobs }

        for proc in psutil.process_iter(['pid', 'cmdline']):
            # Ignore processes which most likely have terminated between the time of
            # iteration and data access.
            with contextlib.suppress(psutil.NoSuchProcess, psutil.AccessDenied):
                if is_plotting_cmdline(proc.cmdline()):
                    if proc.pid in cached_jobs_by_pid.keys():
                        jobs.append(cached_jobs_by_pid[proc.pid])  # Copy from cache
                    else:
                        with proc.oneshot():
                            parsed_command = parse_chia_plots_create_command_line(
                                command_line=proc.cmdline(),
                            )
                            if parsed_command.error is not None:
                                continue
                            job = Job(
                                proc=proc,
                                parsed_command=parsed_command,
                                logroot=logroot,
                            )
                            if job.help:
                                continue
                            jobs.append(job)

        return jobs


    def __init__(self, proc, parsed_command, logroot):
        '''Initialize from an existing psutil.Process object.  must know logroot in order to understand open files'''
        self.proc = proc
        # These are dynamic, cached, and need to be udpated periodically
        self.phase = Phase(known=False)

        self.help = parsed_command.help
        self.args = parsed_command.parameters

        # an example as of 1.0.5
        # {
        #     'size': 32,
        #     'num_threads': 4,
        #     'buckets': 128,
        #     'buffer': 6000,
        #     'tmp_dir': '/farm/yards/901',
        #     'final_dir': '/farm/wagons/801',
        #     'override_k': False,
        #     'num': 1,
        #     'alt_fingerprint': None,
        #     'pool_contract_address': None,
        #     'farmer_public_key': None,
        #     'pool_public_key': None,
        #     'tmp2_dir': None,
        #     'plotid': None,
        #     'memo': None,
        #     'nobitfield': False,
        #     'exclude_final_dir': False,
        # }

        # self.n = self.args['num']
        self.n = self.args['count']
        # self.r = self.args['num_threads']
        self.r = self.args['threads']
        # self.u = self.args['buckets']
        self.u = self.args['buckets']
        # self.tmpdir = self.args['tmp_dir']
        self.tmpdir = self.args['tmpdir']
        # self.tmp2dir = self.args['tmp2_dir']
        self.tmp2dir = self.args['tmpdir2']
        # self.dstdir = self.args['final_dir']
        self.dstdir = self.args['finaldir']
        self.k = "32"   #self.args['size']
        self.b = "4000" #self.args['buffer']

        plot_cwd = self.proc.cwd()
        self.tmpdir = os.path.join(plot_cwd, self.tmpdir)
        if self.tmp2dir is not None:
            self.tmp2dir = os.path.join(plot_cwd, self.tmp2dir)
        self.dstdir = os.path.join(plot_cwd, self.dstdir)

        # Find logfile (whatever file is open under the log root).  The
        # file may be open more than once, e.g. for STDOUT and STDERR.
        for f in self.proc.open_files():
            if logroot in f.path:
                if self.logfile:
                    assert self.logfile == f.path
                else:
                    self.logfile = f.path
                break

        if self.logfile:
            # Initialize data that needs to be loaded from the logfile
            self.init_from_logfile()
        else:
            print('Found plotting process PID {pid}, but could not find '
                    'logfile in its open files:'.format(pid = self.proc.pid))
            for f in self.proc.open_files():
                print(f.path)



    def init_from_logfile(self):
        '''Read plot ID and job start time from logfile.  Return true if we
           find all the info as expected, false otherwise'''
        assert self.logfile
        # Try reading for a while; it can take a while for the job to get started as it scans
        # existing plot dirs (especially if they are NFS).
        found_id = False
        for attempt_number in range(3):
            with open(self.logfile, 'r') as f:
                for line in f:
                    # m = re.match('^ID: ([0-9a-f]*)', line)
                    m = re.match('^Plot Name: (.*)', line)
                    if m:
                        # self.plot_id = m.group(1)
                        filename_parts = m.group(1).split("-")
                        self.plot_id = filename_parts[-1]
                        found_id = True

            if found_id:
                break  # Stop trying
            else:
                time.sleep(1)  # Sleep and try again

        self.start_time = datetime.fromtimestamp(os.path.getctime(self.logfile))

        # Load things from logfile that are dynamic
        self.update_from_logfile()

    def update_from_logfile(self):
        self.set_phase_from_logfile()
        self.check_freeze()

    def set_phase_from_logfile(self):
        assert self.logfile

        # Map from phase number to subphase number reached in that phase.
        # Phase 1 subphases are <started>, table1, table2, ...
        # Phase 2 subphases are <started>, table7, table6, ...
        # Phase 3 subphases are <started>, tables1&2, tables2&3, ...
        # Phase 4 subphases are <started>
        phase_subphases = {}

        with open(self.logfile, 'r') as f:
            for line in f:
                # # "Starting phase 1/4: Forward Propagation into tmp files... Sat Oct 31 11:27:04 2020"
                # m = re.match(r'^Starting phase (\d).*', line)
                # if m:
                #     phase = int(m.group(1))
                #     phase_subphases[phase] = 0                

                # subphases of phase 1
                m = re.match(r'^\[P1\] Table (\d) took.*', line)
                if m:
                    phase_subphases[1] = max(phase_subphases[1], int(m.group(1)))

                # subphases of phase 2
                m = re.match(r'^\[P2\] Table (\d) scan took.*', line)
                if m:
                    phase_subphases[2] = max(phase_subphases[2], 7 - int(m.group(1)))

                # subphases of phase 3
                m = re.match(r'^\[P3-\d\] Table (\d) took.*', line)
                if m:
                    phase_subphases[3] = max(phase_subphases[3], int(m.group(1)))

                # start of phase 4
                m = re.match(r'^\[P4\] Starting to write C1 and C3 tables*', line)
                if m:
                    phase_subphases[4] = 0

                # subphases of phase 4
                m = re.match(r'^\[P4\] Finished writing C1 and C3 tables*', line)
                if m:
                    phase_subphases[4] = 1

                # subphases of phase 4
                m = re.match(r'^\[P4\] Finished writing C2 table*', line)
                if m:
                    phase_subphases[4] = 2

                # # Phase 1: "Computing table 2"
                # m = re.match(r'^Computing table (\d).*', line)
                # if m:
                #     phase_subphases[1] = max(phase_subphases[1], int(m.group(1)))

                # Phase 2: "Backpropagating on table 2"
                # m = re.match(r'^Backpropagating on table (\d).*', line)
                # if m:
                #     phase_subphases[2] = max(phase_subphases[2], 7 - int(m.group(1)))

                # Phase 3: "Compressing tables 4 and 5"
                # m = re.match(r'^Compressing tables (\d) and (\d).*', line)
                # if m:
                #     phase_subphases[3] = max(phase_subphases[3], int(m.group(1)))

                # TODO also collect timing info:

                # "Time for phase 1 = 22796.7 seconds. CPU (98%) Tue Sep 29 17:57:19 2020"
                # for phase in ['1', '2', '3', '4']:
                    # m = re.match(r'^Time for phase ' + phase + ' = (\d+.\d+) seconds..*', line)
                        # data.setdefault....

                # Total time = 49487.1 seconds. CPU (97.26%) Wed Sep 30 01:22:10 2020
                # m = re.match(r'^Total time = (\d+.\d+) seconds.*', line)
                # if m:
                    # data.setdefault(key, {}).setdefault('total time', []).append(float(m.group(1)))

        if phase_subphases:
            phase = max(phase_subphases.keys())
            self.phase = Phase(major=phase, minor=phase_subphases[phase])
        else:
            self.phase = Phase(major=0, minor=0)
    
    def check_freeze(self):
        assert self.logfile
        updated_at = os.path.getmtime(self.logfile)
        now = datetime.now().timestamp() 
        self.time_without_updates_in_min = int((now-updated_at)/60)
    
    def is_frozen(self):
        return self.time_without_updates_in_min > 60

    def progress(self):
        '''Return a 2-tuple with the job phase and subphase (by reading the logfile)'''
        return self.phase

    def plot_id_prefix(self):
        return self.plot_id[:8]

    # TODO: make this more useful and complete, and/or make it configurable
    def status_str_long(self):
        return '{plot_id}\nk={k} r={r} b={b} u={u}\npid:{pid}\ntmp:{tmp}\ntmp2:{tmp2}\ndst:{dst}\nlogfile:{logfile}'.format(
            plot_id = self.plot_id,
            k = self.k,
            r = self.r,
            b = self.b,
            u = self.u,
            pid = self.proc.pid,
            tmp = self.tmpdir,
            tmp2 = self.tmp2dir,
            dst = self.dstdir,
            plotid = self.plot_id,
            logfile = self.logfile
            )

    def get_mem_usage(self):
        return self.proc.memory_info().vms  # Total, inc swapped

    def get_tmp_usage(self):
        total_bytes = 0
        with os.scandir(self.tmpdir) as it:
            for entry in it:
                if self.plot_id in entry.name:
                    try:
                        total_bytes += entry.stat().st_size
                    except FileNotFoundError:
                        # The file might disappear; this being an estimate we don't care
                        pass
        return total_bytes

    def get_run_status(self):
        '''Running, suspended, etc.'''
        status = self.proc.status()
        if status == psutil.STATUS_RUNNING:
            return 'RUN'
        elif status == psutil.STATUS_SLEEPING:
            return 'SLP'
        elif status == psutil.STATUS_DISK_SLEEP:
            return 'DSK'
        elif status == psutil.STATUS_STOPPED:
            return 'STP'
        else:
            return self.proc.status()

    def get_created_time(self):
        return datetime.fromtimestamp(self.proc.create_time()) 
    
    def get_updated_time(self):
        updated_at = os.path.getmtime(self.logfile)
        return datetime.fromtimestamp(updated_at)

    def get_frozen_time_in_mins(self):
        return self.time_without_updates_in_min

    def get_time_wall(self):
        create_time = datetime.fromtimestamp(self.proc.create_time())
        return int((datetime.now() - create_time).total_seconds())

    def get_time_user(self):
        return int(self.proc.cpu_times().user)

    def get_time_sys(self):
        return int(self.proc.cpu_times().system)

    def get_time_iowait(self):
        cpu_times = self.proc.cpu_times()
        iowait = getattr(cpu_times, 'iowait', None)
        if iowait is None:
            return None

        return int(iowait)

    def suspend(self, reason=''):
        self.proc.suspend()
        self.status_note = reason

    def resume(self):
        self.proc.resume()

    def get_temp_files(self):
        # Prevent duplicate file paths by using set.
        temp_files = set([])
        for f in self.proc.open_files():
            if any(
                dir in f.path
                for dir in [self.tmpdir, self.tmp2dir, self.dstdir]
                if dir is not None
            ):
                temp_files.add(f.path)
        
        all_temp_files = os.listdir(self.tmpdir)
        for f in os.listdir(self.tmpdir):
            if self.plot_id in f:
                temp_files.add(os.path.abspath(os.path.join(self.tmpdir, f)))

        return temp_files

    def cancel(self):
        'Cancel an already running job'
        # We typically suspend the job as the first action in killing it, so it
        # doesn't create more tmp files during death.  However, terminate() won't
        # complete if the job is supsended, so we also need to resume it.
        # TODO: check that this is best practice for killing a job.
        self.proc.resume()
        self.proc.terminate()

    def kill(self):
        # First suspend so job doesn't create new files
        self.suspend()

        temp_files = self.get_temp_files()
        self.cancel()
        
        for f in temp_files:
            os.remove(f)

        return True
    