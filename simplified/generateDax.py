#!/usr/bin/env python
import argparse
import os
import Pegasus.DAX3 as peg
import yaml
from collections import defaultdict
from itertools import chain

import lsst.log
import lsst.utils
from lsst.obs.hsc.hscMapper import HscMapper

from data import Data
from pathogen import Pathogen
from cowriter import Cowriter

logger = lsst.log.Log.getLogger('workflow')
logger.setLevel(lsst.log.INFO)

# hard-coded output repo
# A local output repo is written when running this script;
# this local repo is not used at all for actual job submission and run.
# Real submitted run dumps output in scratch (specified in the site catalog).
outPath = 'repo'
logger.debug('outPath: %s', outPath)

# Assuming ci_hsc has been run beforehand and the data repo has been created
ciHscDir = lsst.utils.getPackageDir('ci_hsc')
inputRepo = os.path.join(ciHscDir, 'DATA')


def generateDax(allData, extra, name='dax'):
    """Generate a Pegasus DAX abstract workflow.

    Parameters
    ----------
    allData : `dict`
        Mapping between filters and data to process.
    extra : `dict`
        Any additional data ids required by tasks in the workflow.
    name : `str`, optional
        Name of the workflow DAX, defaults to 'dax'.

    Returns
    -------
    pegasus.ADAG :
        Directed acyclic graph representing the workflow.
    """
    try:
        from AutoADAG import AutoADAG
    except ImportError:
        dax = peg.ADAG(name)
    else:
        dax = AutoADAG(name)

    # Initialize path generator.
    pathogen = Pathogen(HscMapper, root=inputRepo, output=outPath)

    # Initialize generator of Executor's configuration files.
    cowriter = Cowriter('executor')

    # A cache with frequently used files.
    cache = {}

    # Add internal butler files to the cache.
    components = {
        'mapper': '_mapper',
        'registry': 'registry.sqlite3',
        'calibRegistry': 'CALIB/calibRegistry.sqlite3',
    }
    lfns = {k: os.path.join(outPath, v) for k, v in components.items()}
    pfns = {k: os.path.join(inputRepo, v) for k, v in components.items()}
    cache.update(create_cache(lfns, pfns))

    # Add task configuration files to the cache.
    path = os.path.dirname(os.path.realpath(__file__))
    lfns = {
        'makeSkyMapConf': 'skymapConfig.py'
    }
    pfns = {k: os.path.join(path, v) for k, v in lfns.items()}
    cache.update(create_cache(lfns, pfns))

    # The replica catalog containing all DAX-level files.
    catalog = set()

    # Pipeline: processCcd
    for data in chain(*allData.values()):
        name = 'processCcd'

        args = ['--calib', outPath, '--doraise']
        args.extend(data.id())

        ins = set()
        for kind in ['raw', 'bias', 'dark', 'flat', 'bfKernel']:
            lfn, pfn = pathogen.get(kind, data.dataId)
            f = create_file(lfn, pfn)
            ins.add(f)
        ins.update([v for k, v in cache.items()
                    if k in ['mapper', 'registry', 'calibRegistry']])

        lfn = cowriter.write(name, to_str(args), outPath, outPath)
        config = create_file(lfn, os.path.abspath(lfn))
        ins.add(config)

        outs = set()
        for kind in ['calexp', 'src']:
            lfn, pfn = pathogen.get(kind, data.dataId)
            f = create_file(lfn, pfn)
            outs.add(f)

        log = peg.File('log%s.%s' % (name[0].upper() + name[1:], data.name))

        task = create_task('execute', config, ins, outs, log=log)
        dax.addJob(task)

        catalog.update(ins)
        catalog.update(outs)
        catalog.add(log)

        logger.debug('%s dataId: %s' % (name, data.dataId))

    # Pipeline: makeSkyMap
    name = 'makeSkyMap'

    args = ['-C', cache['makeSkyMapConf'], '--doraise']

    ins = set([v for k, v in cache.items()
               if k in ['mapper', 'makeSkyMapConf']])

    lfn = cowriter.write(name, to_str(args), outPath, outPath)
    config = create_file(lfn, os.path.abspath(lfn))
    ins.add(config)

    outs = set()
    lfn, pfn = pathogen.get('deepCoadd_skyMap', {})
    f = create_file(lfn, pfn)
    outs.add(f)
    cache['makeSkyMapOut'] = f

    log = peg.File('log%s' % (name[0].upper() + name[1:]))

    task = create_task('execute', config, ins, outs, log=log)
    dax.addJob(task)

    catalog.update(ins)
    catalog.update(outs)
    catalog.add(log)

    # Create 'exposures' as in ci_hsc/SConstruct processCoadds.
    allExposures = {filterName: defaultdict(list) for filterName in allData}
    for filterName in allData:
        for data in allData[filterName]:
            allExposures[filterName][data.visit].append(data)

    # Pipeline: makeCoaddTempExp per visit per filter
    patchId = ['--id']
    patchId.extend('%s=%s' % (k, v) for k, v in extra.iteritems())
    for filterName, visits in allExposures.items():
        ident = patchId + ['filter=' + filterName]
        for visit, data in visits.items():
            name = 'makeCoaddTempExp'

            args = ['--doraise', '--no-versions', '-c', 'doApplyUberCal=False']
            args.extend(ident)
            args.extend(s for rec in data for s in rec.id('--selectId'))

            ins = set()
            for rec in data:
                lfn, pfn = pathogen.get('calexp', rec.dataId)
                f = create_file(lfn, pfn)
                ins.add(f)
            ins.update([v for k, v in cache.items()
                        if k in ['mapper', 'registry', 'makeSkyMapOut']])

            lfn = cowriter.write(name, to_str(args), outPath, outPath)
            config = create_file(lfn, os.path.abspath(lfn))
            ins.add(config)

            outs = set()
            coaddTempExpId = dict(filter=filterName, visit=visit, **extra)
            lfn, pfn = pathogen.get('deepCoadd_tempExp', coaddTempExpId)
            f = create_file(lfn, pfn)
            outs.add(f)

            log = peg.File(
                'logMakeCoaddTempExp'
                '.%(tract)d-%(patch)s-%(filter)s-%(visit)d' % coaddTempExpId)

            task = create_task('execute', config, ins, outs, log=log)
            dax.addJob(task)

            catalog.update(ins)
            catalog.update(outs)
            catalog.add(log)

            logger.debug(
                'Adding makeCoaddTempExp %s %s %s %s %s %s %s',
                outPath, '--output', outPath, ' --doraise', '--no-versions',
                ident, ' -c doApplyUberCal=False ',
                ' '.join(s for rec in data for s in rec.id('--selectId'))
            )

    for f in catalog:
        dax.addFile(f)
    return dax


def create_cache(lfns, pfns=None):
    """Creates a cache of file entries for DAX-level replica catalog.

    Parameters
    ----------
    lfns : `dict`
        Map between file handles and logical file names.
    pfns : `dict`, optional
        Map between file handles and physical file names. If supplied,
        the function will associate a physical file name with each file.
        Defaults to `None`.

    Returns
    -------
    `dict`
        Map between file handles and DAX-level file entries.
    """
    if pfns is None:
        pfns = {k: None for k in lfns}
    if set(lfns) != set(pfns):
        raise ValueError('logical file name mapping differs from physical.')
    return {k: create_file(lfns[k], pfns[k]) for k in lfns}


def create_file(lfn, pfn=None):
    """Creates Pegasus file entry.

    Parameters
    ----------
    lfn : `str`
        Logical file name.
    pfn : `str`, optional
        Physical file name.

    Returns
    -------
    pegusus.File
        DAX-level file entry.
    """
    f = peg.File(lfn)
    if pfn is not None:
        f.addPFN(peg.PFN(pfn, site='local'))
        f.addPFN(peg.PFN(pfn, site='lsstvc'))
    return f


def create_task(name, args, inputs, outputs, log=None):
    """Creates a fully-fledged Pegasus job.

    Parameters
    ----------
    name : `str`
        The LSST task name.
    args : `list`
        List of task arguments.
    inputs : iterable of Pegasus.File
        Sequence of task's input files.
    inputs : iterable of Pegasus.File
        Sequence of task's output files.
    log : Pegasus.File, optional
        File to which task standard error will be redirected.

    Returns
    -------
    `Pegasus.Job`
        Resource loaded job.
    """
    task = peg.Job(name=name)
    if not isinstance(args, list):
        args = [args]
    task.addArguments(*args)
    for f in inputs:
        task.uses(f, link=peg.Link.INPUT)
    for f in outputs:
        task.uses(f, link=peg.Link.OUTPUT)
    if log is not None:
        task.setStderr(log)
        task.uses(log)
    return task


def to_str(args):
    """Creates a string representation of Job arguments.

    Arguments of a Pegasus Job may contain File objects. As Executor knows
    nothing about Pegasus and its Files, they must be replaced with the
    corresponding logical filename.

    Parameters
    ----------
    args : `list`
        List of arguments for a Pegaus Job.

    Returns
    -------
    `list` of `str`
        List of string representation of the arguemnts.
    """
    indices = [i for i, a in enumerate(args) if isinstance(a, peg.File)]
    for i in indices:
        args[i] = args[i].name
    return args


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate a DAX')
    parser.add_argument('data', help='a file including input data information')
    args = parser.parse_args()

    with open(args.data) as f:
        data = yaml.load(f)
    visits = {filterName: [Data(**dataId) for dataId in dataIds]
              for filterName, dataIds in data['filters'].items()}
    patchDataId = {k: v for k, v in data.items() if k in ['patch', 'tract']}
    dax = generateDax(visits, patchDataId, name='CiHscDax')
    with open('ciHsc.dax', 'w') as f:
        dax.writeXML(f)
