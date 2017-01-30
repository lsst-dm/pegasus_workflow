#!/usr/bin/env python
import argparse
import os
import Pegasus.DAX3 as peg
import yaml
from collections import defaultdict

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

    # Get the following butler or config files directly from ci_hsc package
    filePathMapper = os.path.join(inputRepo, '_mapper')
    mapperFile = peg.File(os.path.join(outPath, '_mapper'))
    mapperFile.addPFN(peg.PFN(filePathMapper, site='local'))
    mapperFile.addPFN(peg.PFN(filePathMapper, site='lsstvc'))
    dax.addFile(mapperFile)

    filePathRegistry = os.path.join(inputRepo, 'registry.sqlite3')
    registry = peg.File(os.path.join(outPath, 'registry.sqlite3'))
    registry.addPFN(peg.PFN(filePathRegistry, site='local'))
    registry.addPFN(peg.PFN(filePathRegistry, site='lsstvc'))
    dax.addFile(registry)

    filePathCalibRegistry = os.path.join(calibRepo, 'calibRegistry.sqlite3')
    calibRegistry = peg.File(os.path.join(outPath, 'calibRegistry.sqlite3'))
    calibRegistry.addPFN(peg.PFN(filePathCalibRegistry, site='local'))
    calibRegistry.addPFN(peg.PFN(filePathCalibRegistry, site='lsstvc'))
    dax.addFile(calibRegistry)

    filePath = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'skymapConfig.py')
    skymapConfig = peg.File('skymapConfig.py')
    skymapConfig.addPFN(peg.PFN(filePath, site='local'))
    skymapConfig.addPFN(peg.PFN(filePath, site='lsstvc'))
    dax.addFile(skymapConfig)

    # Pipeline: processCcd
    tasksProcessCcdList = []

    # Create 'exposures' as in ci_hsc/SConstruct processCoadds
    allExposures = {filterName: defaultdict(list) for filterName in allData}
    for filterName in allData:
        for data in allData[filterName]:
            allExposures[filterName][data.visit].append(data)

    for data in sum(allData.itervalues(), []):
        logger.debug('processCcd dataId: %s', data.dataId)

        processCcd = peg.Job(name='processCcd')
        processCcd.addArguments(outPath, '--calib', outPath, '--output', outPath, ' --doraise', data.id())
        processCcd.uses(registry, link=peg.Link.INPUT)
        processCcd.uses(calibRegistry, link=peg.Link.INPUT)
        processCcd.uses(mapperFile, link=peg.Link.INPUT)

        inFile = getDataFile(mapperInput, 'raw', data.dataId, create=True, replaceRootPath=inputRepo)
        dax.addFile(inFile)
        processCcd.uses(inFile, link=peg.Link.INPUT)
        for inputType in ['bias', 'dark', 'flat', 'bfKernel']:
            inFile = getDataFile(mapperInput, inputType, data.dataId,
                                 create=True, replaceRootPath=calibRepo)
            if not dax.hasFile(inFile):
                dax.addFile(inFile)
            processCcd.uses(inFile, link=peg.Link.INPUT)

        for outputType in ['calexp', 'src']:
            outFile = getDataFile(mapper, outputType, data.dataId, create=True)
            dax.addFile(outFile)
            processCcd.uses(outFile, link=peg.Link.OUTPUT)

        logProcessCcd = peg.File('logProcessCcd.%s' % data.name)
        dax.addFile(logProcessCcd)
        processCcd.setStderr(logProcessCcd)
        processCcd.uses(logProcessCcd, link=peg.Link.OUTPUT)

        dax.addJob(processCcd)
        tasksProcessCcdList.append(processCcd)

    # Pipeline: makeSkyMap
    makeSkyMap = peg.Job(name='makeSkyMap')
    makeSkyMap.uses(mapperFile, link=peg.Link.INPUT)
    makeSkyMap.uses(skymapConfig, link=peg.Link.INPUT)
    makeSkyMap.addArguments(outPath, '--output', outPath, '-C', skymapConfig, ' --doraise')
    logMakeSkyMap = peg.File('logMakeSkyMap')
    dax.addFile(logMakeSkyMap)
    makeSkyMap.setStderr(logMakeSkyMap)
    makeSkyMap.uses(logMakeSkyMap, link=peg.Link.OUTPUT)

    skyMap = getDataFile(mapper, 'deepCoadd_skyMap', {}, create=True)
    dax.addFile(skyMap)
    makeSkyMap.uses(skyMap, link=peg.Link.OUTPUT)

    dax.addJob(makeSkyMap)

    # Pipeline: makeCoaddTempExp per visit per filter
    patchId = ' '.join(('%s=%s' % (k, v) for k, v in extra.iteritems()))
    for filterName in allExposures:
        ident = '--id ' + patchId + ' filter=' + filterName
        coaddTempExpList = []
        for visit in allExposures[filterName]:
            makeCoaddTempExp = peg.Job(name='makeCoaddTempExp')
            makeCoaddTempExp.uses(mapperFile, link=peg.Link.INPUT)
            makeCoaddTempExp.uses(registry, link=peg.Link.INPUT)
            makeCoaddTempExp.uses(skyMap, link=peg.Link.INPUT)
            for data in allExposures[filterName][visit]:
                calexp = getDataFile(mapper, 'calexp', data.dataId, create=False)
                makeCoaddTempExp.uses(calexp, link=peg.Link.INPUT)

            makeCoaddTempExp.addArguments(
                outPath, '--output', outPath, ' --doraise', '--no-versions',
                ident, ' -c doApplyUberCal=False ',
                ' '.join(data.id('--selectId') for data in allExposures[filterName][visit])
            )
            logger.debug(
                'Adding makeCoaddTempExp %s %s %s %s %s %s %s',
                outPath, '--output', outPath, ' --doraise', '--no-versions',
                ident, ' -c doApplyUberCal=False ',
                ' '.join(data.id('--selectId') for data in allExposures[filterName][visit])
            )

            coaddTempExpId = dict(filter=filterName, visit=visit, **patchDataId)
            logMakeCoaddTempExp = peg.File(
                'logMakeCoaddTempExp.%(tract)d-%(patch)s-%(filter)s-%(visit)d' % coaddTempExpId)
            dax.addFile(logMakeCoaddTempExp)
            makeCoaddTempExp.setStderr(logMakeCoaddTempExp)
            makeCoaddTempExp.uses(logMakeCoaddTempExp, link=peg.Link.OUTPUT)

            deepCoadd_tempExp = getDataFile(mapper, 'deepCoadd_tempExp', coaddTempExpId, create=True)
            dax.addFile(deepCoadd_tempExp)
            makeCoaddTempExp.uses(deepCoadd_tempExp, link=peg.Link.OUTPUT)
            coaddTempExpList.append(deepCoadd_tempExp)

            dax.addJob(makeCoaddTempExp)

    return dax


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
    f = open('ciHsc.dax', 'w')
    dax.writeXML(f)
    f.close()
