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
calibRepo = os.path.join(inputRepo, 'CALIB')


def getDataFile(mapper, datasetType, dataId, create=False, replaceRootPath=None):
    """Get the Pegasus File entry given Butler datasetType and dataId.

    Retrieve the file name/path through a CameraMapper instance
    Optionally tweak the path to a better LFN using replaceRootPath
    Optionally create new Pegasus File entries

    Parameters
    ----------
    mapper: lsst.obs.base.CameraMapper
        A specific CameraMapper instance for getting the name and locating
        the file in a Butler repo.
    datasetType: `str`
        Butler dataset type
    dataId: `dict`
        Butler data ID
    create: `bool`, optional
        If True, create a new Pegasus File entry if it does not exist yet.
    replaceRootPath: `str`, optional
        Replace the given root path with the global outPath.

    Returns
    -------
    fileEntry:
        A Pegasus File entry or a LFN corresponding to an entry
    """
    mapFunc = getattr(mapper, 'map_' + datasetType)
    fileEntry = lfn = filePath = mapFunc(dataId).getLocations()[0]

    if replaceRootPath is not None:
        lfn = filePath.replace(replaceRootPath, outPath)

    if create:
        fileEntry = peg.File(lfn)
        if not filePath.startswith(outPath):
            fileEntry.addPFN(peg.PFN(filePath, site='local'))
            fileEntry.addPFN(peg.PFN(filePath, site='lsstvc'))
        logger.debug('%s %s: %s -> %s', datasetType, dataId, filePath, lfn)

    return fileEntry


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

    # Construct these mappers only for creating dax, not for actual runs.
    mapperInput = HscMapper(root=inputRepo)
    mapper = HscMapper(root=inputRepo, outputRoot=outPath)

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
    cache.update(create_files(lfns, pfns))

    # Add task configuration files to the cache.
    path = os.path.dirname(os.path.realpath(__file__))
    lfns = {
        'makeSkyMapConf': 'skymapConfig.py'
    }
    pfns = {k: os.path.join(path, v) for k, v in lfns.items()}
    cache.update(create_files(lfns, pfns))

    # The replica catalog containing all DAX-level files.
    catalog = set()

    # Pipeline: processCcd
    for data in chain(*allData.values()):
        name = 'processCcd'

        args = [
            outPath, '--calib', outPath, '--output', outPath, '--doraise',
            data.id()
        ]

        ins = set()
        f = getDataFile(mapperInput, 'raw', data.dataId, create=True,
                        replaceRootPath=inputRepo)
        ins.add(f)
        for kind in ['bias', 'dark', 'flat', 'bfKernel']:
            f = getDataFile(mapperInput, kind, data.dataId, create=True,
                            replaceRootPath=calibRepo)
            ins.add(f)
        ins.update([v for k, v in cache.items()
                    if k in ['mapper', 'registry', 'calibRegistry']])

        outs = set()
        for kind in ['calexp', 'src']:
            f = getDataFile(mapper, kind, data.dataId, create=True)
            outs.add(f)

        log = peg.File('log%s.%s' % (name.capitalize(), data.name))

        task = create_task(name, args, ins, outs, log=log)
        dax.addJob(task)

        catalog.update(ins)
        catalog.update(outs)
        catalog.add(log)

        logger.debug('%s dataId: %s' % (name, data.dataId))

    # Pipeline: makeSkyMap
    name = 'makeSkyMap'

    args = [
        outPath, '--output', outPath, '-C', cache['makeSkyMapConf'], '--doraise'
    ]

    ins = set([v for k, v in cache.items()
               if k in ['mapper', 'makeSkyMapConf']])

    outs = set()
    f = getDataFile(mapper, 'deepCoadd_skyMap', {}, create=True)
    outs.add(f)
    cache['makeSkyMapOut'] = f

    log = peg.File('log%s' % (name.capitalize()))

    task = create_task(name, args, ins, outs, log=log)
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
    patchId = ' '.join(('%s=%s' % (k, v) for k, v in extra.iteritems()))
    for filterName, visits in allExposures.items():
        ident = '--id ' + patchId + ' filter=' + filterName
        for visit, data in visits.items():
            name = 'makeCoaddTempExp'

            args = [
                outPath, '--output', outPath, ' --doraise', '--no-versions',
                ident, ' -c doApplyUberCal=False ',
                ' '.join(rec.id('--selectId') for rec in data)
            ]

            ins = set()
            for rec in data:
                f = getDataFile(mapper, 'calexp', rec.dataId, create=True)
                ins.add(f)
            ins.update([v for k, v in cache.items()
                        if k in ['mapper', 'registry', 'makeSkyMapOut']])

            outs = set()
            coaddTempExpId = dict(filter=filterName, visit=visit, **patchDataId)
            f = getDataFile(mapper, 'deepCoadd_tempExp', coaddTempExpId,
                            create=True)
            outs.add(f)

            log = peg.File(
                'logMakeCoaddTempExp'
                '.%(tract)d-%(patch)s-%(filter)s-%(visit)d' % coaddTempExpId)

            task = create_task(name, args, ins, outs, log=log)
            dax.addJob(task)

            catalog.update(ins)
            catalog.update(outs)
            catalog.add(log)

            logger.debug(
                'Adding makeCoaddTempExp %s %s %s %s %s %s %s',
                outPath, '--output', outPath, ' --doraise', '--no-versions',
                ident, ' -c doApplyUberCal=False ',
                ' '.join(rec.id('--selectId') for rec in data)
            )

    for f in catalog:
        dax.addFile(f)
    return dax


def create_files(lfns, pfns=None):
    """Creates file entries for DAX-level replica catalog.

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
    files = {handle: peg.File(name) for handle, name in lfns.items()}
    if pfns is not None:
        if set(lfns) != set(pfns):
            raise ValueError('logical file name mapping differs from physical.')
        for handle, entry in files.items():
            entry.addPFN(peg.PFN(pfns[handle], site='local'))
            entry.addPFN(peg.PFN(pfns[handle], site='lsstvc'))
    return files


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
    task.addArguments(*args)
    for f in inputs:
        task.uses(f, link=peg.Link.INPUT)
    for f in outputs:
        task.uses(f, link=peg.Link.OUTPUT)
    if log is not None:
        task.setStderr(log)
        task.uses(log)
    return task


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
