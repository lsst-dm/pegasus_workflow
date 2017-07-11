#!/usr/bin/env python
import argparse
import os
import Pegasus.DAX3 as peg

import lsst.log
import lsst.utils
from lsst.utils import getPackageDir
from lsst.daf.persistence import Butler
from lsst.obs.hsc.hscMapper import HscMapper
from findShardId import findShardId

logger = lsst.log.Log.getLogger("workflow")
logger.setLevel(lsst.log.WARN)

# hard-coded output repo
# A local output repo is written when running this script;
# this local repo is not used at all for actual job submission and run.
# Real submitted run dumps output in scratch (specified in the site catalog).
outPath = 'repoCo'
logger.debug("outPath: %s", outPath)

inputRepo = "/project/hsc_rc/w_2017_28/DM-11184/"
rootRepo = "/datasets/hsc/repo"

# This is a config of LoadIndexedReferenceObjectsTask ref_dataset_name
refcatName = "ps1_pv3_3pi_20170110"

def getDataFile(mapper, datasetType, dataId, create=False, repoRoot=None):
    """Get the Pegasus File entry given Butler datasetType and dataId.
    Retrieve the file name/path through a CameraMapper instance
        and prepend outPath to it
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
    repoRoot: `str`, optional
        Prepend butler repo root path for the PFN of the input files
    Returns
    -------
    fileEntry:
        A Pegasus File entry or a LFN corresponding to an entry
    """
    mapFunc = getattr(mapper, "map_" + datasetType)
    butlerPath = mapFunc(dataId).getLocations()[0]
    fileEntry = lfn = os.path.join(outPath, butlerPath)

    if create:
        fileEntry = peg.File(lfn)
        if repoRoot is not None:
            filePath = os.path.join(repoRoot, butlerPath)
            #fileEntry.addPFN(peg.PFN(filePath, site="local"))
            fileEntry.addPFN(peg.PFN(filePath, site="lsstvc"))
            logger.info("%s %s: %s -> %s", datasetType, dataId, filePath, lfn)

    return fileEntry

def generateCoaddDax(name="dax", tractDataId=0, dataDict=None):
    """Generate a Pegasus DAX abstract workflow"""
    try:
        from AutoADAG import AutoADAG
    except ImportError:
        dax = peg.ADAG(name)
    else:
        dax = AutoADAG(name)

    # Construct these mappers only for creating dax, not for actual runs.
    mapper = HscMapper(root=rootRepo)

    # Get the following butler or config files directly from ci_hsc package
    filePathMapper = os.path.join(rootRepo, "_mapper")
    mapperFile = peg.File(os.path.join(outPath, "_mapper"))
    mapperFile.addPFN(peg.PFN(filePathMapper, site="local"))
    mapperFile.addPFN(peg.PFN(filePathMapper, site="lsstvc"))
    dax.addFile(mapperFile)

    filePathRegistry = os.path.join(rootRepo, "registry.sqlite3")
    registry = peg.File(os.path.join(outPath, "registry.sqlite3"))
    registry.addPFN(peg.PFN(filePathRegistry, site="local"))
    registry.addPFN(peg.PFN(filePathRegistry, site="lsstvc"))
    dax.addFile(registry)

    # Add all files in ref_cats
    refCatConfigFile = getDataFile(mapper, "ref_cat_config", {"name": refcatName}, create=True, repoRoot=rootRepo)
    dax.addFile(refCatConfigFile)

    refCatSchema = "ref_cats/ps1_pv3_3pi_20170110/master_schema.fits"
    filePath = os.path.join(rootRepo, refCatSchema)
    refCatSchemaFile = peg.File(os.path.join(outPath, refCatSchema))
    refCatSchemaFile.addPFN(peg.PFN(filePath, site="local"))
    refCatSchemaFile.addPFN(peg.PFN(filePath, site="lsstvc"))
    dax.addFile(refCatSchemaFile)

    # pre-run detectCoaddSources for schema
    preDetectCoaddSources = peg.Job(name="detectCoaddSources")
    preDetectCoaddSources.uses(mapperFile, link=peg.Link.INPUT)
    preDetectCoaddSources.addArguments(outPath, "--output", outPath, " --doraise")
    deepCoadd_det_schema = getDataFile(mapper, "deepCoadd_det_schema", {}, create=True)
    dax.addFile(deepCoadd_det_schema)
    preDetectCoaddSources.uses(deepCoadd_det_schema, link=peg.Link.OUTPUT)
    dax.addJob(preDetectCoaddSources)

    # Pipeline: processCcd  -- DONE previously
    # Pipeline: makeSkyMap  -- DONE previously
    skyMap = getDataFile(mapper, "deepCoadd_skyMap", {}, create=True, repoRoot=inputRepo)
    dax.addFile(skyMap)

    # Pipeline: makeCoaddTempExp per patch per visit per filter
    for filterName in dataDict:
        for patchDataId in dataDict[filterName]:
            ident = "--id tract=%s patch=%s filter=%s" % (tractDataId, patchDataId, filterName)
            coaddTempExpList = []
            visitDict = defaultdict(list)
            for ccd in dataDict[filterName][patchDataId]:
                visitId, ccdId = map(int, ccd.split('-'))
                visitDict[visitId].append(ccdId)
            for visitId in visitDict:
                makeCoaddTempExp = peg.Job(name="makeCoaddTempExp")
                makeCoaddTempExp.uses(mapperFile, link=peg.Link.INPUT)
                makeCoaddTempExp.uses(registry, link=peg.Link.INPUT)
                makeCoaddTempExp.uses(skyMap, link=peg.Link.INPUT)
                for ccdId in visitDict[visitId]:
                    calexp = getDataFile(mapper, "calexp", {'visit': visitId, 'ccd': ccdId},
                                         create=True, repoRoot=inputRepo)
                    if not dax.hasFile(calexp):
                        dax.addFile(calexp)
                    makeCoaddTempExp.uses(calexp, link=peg.Link.INPUT)

                makeCoaddTempExp.addArguments(
                    outPath, "--output", outPath, " --doraise",
                    ident, " -c doApplyUberCal=False ",
                    "--selectId visit=%s ccd=%s" % (visitId, '^'.join(str(ccdId) for ccdId in visitDict[visitId]))
                )
                logger.debug(
                    "Adding makeCoaddTempExp %s %s %s %s %s %s %s",
                    outPath, "--output", outPath, " --doraise",
                    ident, " -c doApplyUberCal=False ",
                    "--selectId visit=%s ccd=%s" % (visitId, '^'.join(str(ccdId) for ccdId in visitDict[visitId]))
                )

                coaddTempExpId = dict(filter=filterName, visit=visitId, tract=tractDataId, patch=patchDataId)
                logMakeCoaddTempExp = peg.File(
                    "logMakeCoaddTempExp.%(tract)d-%(patch)s-%(filter)s-%(visit)d" % coaddTempExpId)
                dax.addFile(logMakeCoaddTempExp)
                makeCoaddTempExp.setStderr(logMakeCoaddTempExp)
                makeCoaddTempExp.uses(logMakeCoaddTempExp, link=peg.Link.OUTPUT)

                deepCoadd_directWarp = getDataFile(mapper, "deepCoadd_directWarp", coaddTempExpId, create=True)
                dax.addFile(deepCoadd_directWarp)
                makeCoaddTempExp.uses(deepCoadd_directWarp, link=peg.Link.OUTPUT)
                coaddTempExpList.append(deepCoadd_directWarp)

                dax.addJob(makeCoaddTempExp)

            # Pipeline: assembleCoadd per patch per filter
            assembleCoadd = peg.Job(name="assembleCoadd")
            assembleCoadd.uses(mapperFile, link=peg.Link.INPUT)
            assembleCoadd.uses(registry, link=peg.Link.INPUT)
            assembleCoadd.uses(skyMap, link=peg.Link.INPUT)
            assembleCoadd.addArguments(
                outPath, "--output", outPath, ident, " --doraise",
                " --selectId visit=" + " --selectId visit=".join(str(visitId) for visitId in visitDict)
            )
            logger.debug(
                "Adding assembleCoadd %s %s %s %s %s %s",
                outPath, "--output", outPath, ident, " --doraise",
                " --selectId visit=" + " --selectId visit=".join(str(visitId) for visitId in visitDict)
            )

            # calexp_md is used in SelectDataIdContainer
            for visitId in visitDict:
                for ccdId in visitDict[visitId]:
                    calexp = getDataFile(mapper, "calexp", {'visit': visitId, 'ccd': ccdId}, create=False)
                    assembleCoadd.uses(calexp, link=peg.Link.INPUT)

            for coaddTempExp in coaddTempExpList:
                assembleCoadd.uses(coaddTempExp, link=peg.Link.INPUT)

            coaddId = dict(filter=filterName, tract=tractDataId, patch=patchDataId)
            logAssembleCoadd = peg.File("logAssembleCoadd.%(tract)d-%(patch)s-%(filter)s" % coaddId)
            dax.addFile(logAssembleCoadd)
            assembleCoadd.setStderr(logAssembleCoadd)
            assembleCoadd.uses(logAssembleCoadd, link=peg.Link.OUTPUT)

            coadd = getDataFile(mapper, "deepCoadd", coaddId, create=True)
            dax.addFile(coadd)
            assembleCoadd.uses(coadd, link=peg.Link.OUTPUT)
            dax.addJob(assembleCoadd)

            # Pipeline: detectCoaddSources each coadd (per patch per filter)
            detectCoaddSources = peg.Job(name="detectCoaddSources")
            detectCoaddSources.uses(mapperFile, link=peg.Link.INPUT)
            detectCoaddSources.uses(coadd, link=peg.Link.INPUT)
            detectCoaddSources.addArguments(outPath, "--output", outPath, ident, " --doraise")

            logDetectCoaddSources = peg.File(
                "logDetectCoaddSources.%(tract)d-%(patch)s-%(filter)s" % coaddId)
            dax.addFile(logDetectCoaddSources)
            detectCoaddSources.setStderr(logDetectCoaddSources)
            detectCoaddSources.uses(logDetectCoaddSources, link=peg.Link.OUTPUT)

            inFile = getDataFile(mapper, "deepCoadd_det_schema", {}, create=False)
            detectCoaddSources.uses(inFile, link=peg.Link.INPUT)
            for outputType in ["deepCoadd_calexp", "deepCoadd_calexp_background", "deepCoadd_det"]:
                outFile = getDataFile(mapper, outputType, coaddId, create=True)
                dax.addFile(outFile)
                detectCoaddSources.uses(outFile, link=peg.Link.OUTPUT)

            dax.addJob(detectCoaddSources)

    return dax


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a DAX")
    parser.add_argument("-t", "--tractId", type=int, default=8766,
                        help="the tract ID of the input file")
    parser.add_argument("-i", "--inputData", default="rcHsc/smallFPVC_t8766",
                        help="a file including input data information")
    parser.add_argument("-o", "--outputFile", type=str, default="HscRcTest.dax",
                        help="file name for the output dax xml")
    args = parser.parse_args()
    from collections import defaultdict
    dataDict = defaultdict(dict)
    # dataDict[filterName][patch] is a list of 'visit-ccd'
    with open(args.inputData, "r") as f:
        for line in f:
            filterName, patchId, visitCcd = line.rstrip().split('|')
            dataDict[filterName][patchId] = visitCcd.split(',')

    logger.debug("dataDict: %s", dataDict)
    dax = generateCoaddDax("HscCoaddDax", args.tractId, dataDict)
    with open(args.outputFile, "w") as f:
        dax.writeXML(f)
