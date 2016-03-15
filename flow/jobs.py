"""This module defines the general workflow for operating on the workspace.

Functions defined in this module are called from the run.py module.
Furthermore we provide functions to classify jobs and determine the
next operation."""
import os
import six
import logging
import json
from datetime import datetime

import signac

import constants
from util.misc import cast_json

logger = logging.getLogger(__name__)


def num_iterations(msibi):
    iterations = msibi.get('iteration', {'-1': None})
    return max((int(k) for k in iterations.keys()))


def get_msibi_version(job):
    fn_log = os.path.join(job.workspace(), 'run.log')
    with open(fn_log) as logfile:
        for line in logfile:
            if 'Executing msibi' in line:
                tokens = line.split()
                version_tuple = tokens[-1][1:-1].split('.')
                if 'develop' in version_tuple:
                    yield (0, 0, 0)
                else:
                    yield tuple((int(v) for v in version_tuple))


def classify(job):
    "Classify this job by yielding 'labels' based on the job data."
    states = job.statepoint().get('states')
    if states is None:
        return
    if not len(states):
        yield 'nostates'
    if 'systems' in job.document:
        yield 'has_systems'
    if job.isfile('msibi.json'):
        yield 'initialized'
        msibi_versions = set(get_msibi_version(job))
        if msibi_versions:
            tag = 'msibi_v{}'.format('.'.join((str(s) for s in max(msibi_versions))))
            if len(msibi_versions) > 1:
                tag += '*'
            yield tag
        with open(os.path.join(job.workspace(), 'msibi.json')) as file:
            msibi = json.load(file)
            iteration = num_iterations(msibi)
            if iteration > 1:
                yield 'msibistarted'
            if iteration > 5:
                yield 'msibiwellbehaved'
            if iteration >= constants.MAX_NUM_MSIBI_ITERATIONS:
                yield 'msibistepslimited'
            fitness = msibi.get('max_mean_fitness', 0)
            if fitness > 0.9:
                yield 'msibiconverged'
            iteration = msibi.get('num_iterations', 0)
    log_status = job.document.get('log_status', set())
    if 'NaN' in log_status:
        yield 'exploded'
    if 'WALLTIME' in log_status:
        yield 'walltimestop'
    if 'I/O error' in log_status:
        yield 'ioerror'


def next_job(job):
    "Determine the next job, based on the job's data."
    states = job.statepoint().get('states')
    if not states:
        return
    labels = set(classify(job))
    if not 'has_systems' in labels:
        return 'pre_init'
    if 'postassembled' in labels:
        return  # That's it at this point.
    if 'msibiconverged' in labels:
        return 'post_selfassemble'
    if 'initialized' in labels:
        return 'converge'
    return 'initialize'


# def add_num_particles(statepoint, init=True, args=None):
#    import glotzformats
#    import util
#    import os
#    pos_reader = glotzformats.reader.PosFileReader()
#    project = signac.contrib.get_project()
#    with project.open_job(statepoint) as job:
#        system = job.document['system']
#        docs = util.find_trajectories(job.statepoint()['shape_id'])
#        for doc in docs:
#            fn_pos = os.path.join(doc['root'], doc['filename'])
#            with open(fn_pos) as posfile:
#                traj = pos_reader.read(posfile)
#                pf = int(float(doc['packing_fraction']) /
#                         doc['packing_fraction_factor'] * 1000)
#                s = system[str(pf)]
#                s['N'] = len(traj[-1])
#                system[str(pf)] = s
#        job.document['system'] = system


def redirect_log_file(job):
    import hoomd_script as hoomd
    cleanup_log_file(job)
    fn_log = os.path.join(job.workspace(), 'hoomd.log'.format(job))
    fn_log_tmp = fn_log + '.tmp'
    if hoomd.comm.get_partition() == 0 and hoomd.comm.get_rank() == 0:
        try:
            with open(fn_log, 'rb') as logfile:
                with open(fn_log_tmp, 'ab') as tmplogfile:
                    tmplogfile.write(logfile.read())
        except IOError as error:
            print(error)
        hoomd.option.set_msg_file(fn_log)


def cleanup_log_file(job):
    import hoomd_script as hoomd
    fn_log = os.path.join(job.workspace(), '{}.log'.format(job))
    fn_log_tmp = fn_log + '.tmp'
    if hoomd.comm.get_partition() == 0 and hoomd.comm.get_rank() == 0:
        try:
            with open(fn_log, 'rb') as logfile:
                with open(fn_log_tmp, 'ab') as tmplogfile:
                    tmplogfile.write(logfile.read())
                    tmplogfile.flush()
            if six.PY2:
                os.rename(fn_log_tmp, fn_log)
            else:
                os.replace(fn_log_tmp, fn_log)
        except IOError as error:
            print(error)


def store_meta_data(job):
    from hoomd_script import meta, init
    if not init.is_initialized():
        return
    metadata = job.document.get('hoomd_meta', dict())
    metadata[int(datetime.now().timestamp())] = cast_json(meta.dump_metadata())
    job.document['hoomd_meta'] = metadata


def testjob(job, init=True, args=None):
    """Try to initialize hoomd-blue for testing purposes.

    Don't actually do anything."""
    print('start test job')
    import hoomd_script as hoomd
    print('imported hoomd-blue')
    if init:
        hoomd.context.initialize('--mode=gpu --nrank=1')
    print('initialized hoomd-blue')
    hoomd.init.create_random(N=100, phi_p=0.1)
    print('created system')
    lj = hoomd.pair.lj(r_cut=2.5)
    lj.pair_coeff.set('A', 'A', epsilon=1.0, sigma=1.0)
    hoomd.integrate.mode_standard(dt=0.005)
    hoomd.integrate.nvt(group=hoomd.group.all(), T=1.2, tau=0.5)
    hoomd.run(1e5)
    print('done')

def pre_init(job, init=True):
    from calc_targets import calc_target
    logging.info("{} initializing".format(job))
    if not 'states' in job.statepoint():
        logger.debug("Skipping, no states defined.")
        return
    states = job.statepoint()['states']
    if 'systems' in job.document:
        logger.info("systems variable already present.")
        return
    systems = []
    try:
        for i, state in enumerate(states):
            N, r_cut, box, target = calc_target(state)
            systems.append(dict(N=N, r_cut=r_cut, box=box.__dict__, target=target))
        job.document['systems'] = systems
    except RuntimeError as error:
        logger.error(error)

def initialize(job, init=True):
    import msibi
    fn_msibi = os.path.join(job.workspace(), 'msibi.json')
    converger = msibi.BaseConverger(restart=fn_msibi)
    states = job.statepoint().get('states')
    if states is None:
        logger.debug("Not states defined, skipping.")
        return
    systems = job.document['systems']
    assert len(states) == len(systems)
    for state, system in zip(states, systems):
        target = system.pop('target')
        state.update(system)
        sp = job.statepoint().copy()
        del sp['states']
        state.update(sp)
        converger.add_state(state, target)
    converger.initialize()


def converge(job, init=True, args=None):
    "This job applies the MS-IBI method."
    import hoomd_script as hoomd
    if init:
        hoomd.context.initialize('--mode=gpu --nrank=1')
    import convergence
    redirect_log_file(job)
    try:
        return convergence.converge(job)
    finally:
        cleanup_log_file(job)
        store_meta_data(job)
