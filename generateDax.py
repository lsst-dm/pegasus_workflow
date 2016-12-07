#!/usr/bin/env python
import os
import Pegasus.DAX3 as peg

import lsst.daf.persistence as dafPersist
import lsst.log
import lsst.utils
from lsst.obs.hsc.hscMapper import HscMapper
from inputData import *

logger = lsst.log.Log.getLogger("workflow")
logger.setLevel(lsst.log.DEBUG)

# hard-coded output repo
# A local output repo is written when running this script;
# this local repo is not used at all for actual job submission and run.
# Real submitted run dumps output in scratch (specified in the site catalog).
outPath = 'peg'
logger.debug("outPath: %s", outPath)

# Assuming ci_hsc has been run beforehand and the data repo has been created
ciHscDir = lsst.utils.getPackageDir('ci_hsc')
inputRepo = os.path.join(ciHscDir, "DATA")
calibRepo = os.path.join(inputRepo, "CALIB")


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
    mapFunc = getattr(mapper, "map_" + datasetType)
    fileEntry = lfn = filePath = mapFunc(dataId).getLocations()[0]

    if replaceRootPath is not None:
        lfn = filePath.replace(replaceRootPath, outPath)

    if create:
        fileEntry = peg.File(lfn)
        fileEntry.addPFN(peg.PFN(filePath, site="local"))
        logger.debug("%s %s: %s -> %s", datasetType, dataId, filePath, lfn)

    return fileEntry


def generateDax(name="dax"):
    """Generate a Pegasus DAX abstract workflow"""
    dax = peg.ADAG(name)

    # Construct these mappers only for creating dax, not for actual runs.
    inputArgs = dafPersist.RepositoryArgs(
        mode='r', mapper=HscMapper, root=inputRepo)  # read-only input
    outputArgs = dafPersist.RepositoryArgs(
        mode='w', mapper=HscMapper, root=outPath)  # write-only output
    mapperInput = HscMapper(root=inputRepo)
    mapper = HscMapper(root=inputRepo, outputRoot=outPath)

    # Get the following butler or config files directly from ci_hsc package
    filePathMapper = os.path.join(inputRepo, "_mapper")
    mapperFile = peg.File(os.path.join(outPath, "_mapper"))
    mapperFile.addPFN(peg.PFN(filePathMapper, site="local"))
    dax.addFile(mapperFile)

    filePathRegistry = os.path.join(inputRepo, "registry.sqlite3")
    registry = peg.File(os.path.join(outPath, "registry.sqlite3"))
    registry.addPFN(peg.PFN(filePathRegistry, site="local"))
    dax.addFile(registry)

    filePathCalibRegistry = os.path.join(calibRepo, "calibRegistry.sqlite3")
    calibRegistry = peg.File(os.path.join(outPath, "calibRegistry.sqlite3"))
    calibRegistry.addPFN(peg.PFN(filePathCalibRegistry, site="local"))
    dax.addFile(calibRegistry)

    filePath = os.path.join(ciHscDir, "skymap.py")
    skymapConfig = peg.File("skymap.py")
    skymapConfig.addPFN(peg.PFN(filePath, site="local"))
    dax.addFile(skymapConfig)

    filePath = os.path.join(ciHscDir, "forcedPhotCcdConfig.py")
    forcedPhotCcdConfig = peg.File("forcedPhotCcdConfig.py")
    forcedPhotCcdConfig.addPFN(peg.PFN(filePath, site="local"))
    dax.addFile(forcedPhotCcdConfig)

    # Pipeline: processCcd
    tasksProcessCcdList = []

    for data in sum(allData.itervalues(), []):
        logger.debug("processCcd dataId: %s", data.dataId)

        processCcd = peg.Job(name="processCcd")
        processCcd.addArguments(outPath, "--calib", outPath, "--output", outPath,
                                " --doraise", data.id())
        processCcd.uses(registry, link=peg.Link.INPUT)
        processCcd.uses(calibRegistry, link=peg.Link.INPUT)
        processCcd.uses(mapperFile, link=peg.Link.INPUT)

        inFile = getDataFile(mapperInput, "raw", data.dataId, create=True, replaceRootPath=inputRepo)
        dax.addFile(inFile)
        processCcd.uses(inFile, link=peg.Link.INPUT)
        for inputType in ["bias", "dark", "flat", "bfKernel"]:
            inFile = getDataFile(mapperInput, inputType, data.dataId,
                                 create=True, replaceRootPath=calibRepo)
            if not dax.hasFile(inFile):
                dax.addFile(inFile)
            processCcd.uses(inFile, link=peg.Link.INPUT)

        for outputType in ["calexp", "src"]:
            outFile = getDataFile(mapper, outputType, data.dataId, create=True)
            dax.addFile(outFile)
            processCcd.uses(outFile, link=peg.Link.OUTPUT)

        logProcessCcd = peg.File("logProcessCcd.%s" % data.name)
        dax.addFile(logProcessCcd)
        processCcd.setStderr(logProcessCcd)
        processCcd.uses(logProcessCcd, link=peg.Link.OUTPUT)

        dax.addJob(processCcd)
        tasksProcessCcdList.append(processCcd)

    # Pipeline: makeSkyMap
    makeSkyMap = peg.Job(name="makeSkyMap")
    makeSkyMap.uses(mapperFile, link=peg.Link.INPUT)
    makeSkyMap.uses(registry, link=peg.Link.INPUT)
    makeSkyMap.uses(skymapConfig, link=peg.Link.INPUT)
    makeSkyMap.addArguments(outPath, "--output", outPath, "-C", skymapConfig, " --doraise")
    logMakeSkyMap = peg.File("logMakeSkyMap")
    dax.addFile(logMakeSkyMap)
    makeSkyMap.setStderr(logMakeSkyMap)
    makeSkyMap.uses(logMakeSkyMap, link=peg.Link.OUTPUT)

    skyMap = getDataFile(mapper, "deepCoadd_skyMap", {}, create=True)
    dax.addFile(skyMap)
    makeSkyMap.uses(skyMap, link=peg.Link.OUTPUT)

    dax.addJob(makeSkyMap)

    # Pipeline: makeCoaddTempExp per visit per filter
    for filterName in allExposures:
        ident = "--id " + patchId + " filter=" + filterName
        coaddTempExpList = []
        for visit in allExposures[filterName]:
            makeCoaddTempExp = peg.Job(name="makeCoaddTempExp")
            makeCoaddTempExp.uses(mapperFile, link=peg.Link.INPUT)
            makeCoaddTempExp.uses(skyMap, link=peg.Link.INPUT)
            for data in allExposures[filterName][visit]:
                calexp = getDataFile(mapper, "calexp", data.dataId, create=False)
                makeCoaddTempExp.uses(calexp, link=peg.Link.INPUT)

            makeCoaddTempExp.addArguments(
                outPath, "--output", outPath, " --doraise",
                ident, " -c doApplyUberCal=False ",
                " ".join(data.id("--selectId") for data in allExposures[filterName][visit])
            )
            logger.debug(
                "Adding makeCoaddTempExp %s %s %s %s %s %s %s",
                outPath, "--output", outPath, " --doraise",
                ident, " -c doApplyUberCal=False ",
                " ".join(data.id("--selectId") for data in allExposures[filterName][visit])
            )

            coaddTempExpId = dict(filter=filterName, visit=visit, **patchDataId)
            logMakeCoaddTempExp = peg.File(
                "logMakeCoaddTempExp.%(tract)d-%(patch)s-%(filter)s-%(visit)d" % coaddTempExpId)
            dax.addFile(logMakeCoaddTempExp)
            makeCoaddTempExp.setStderr(logMakeCoaddTempExp)
            makeCoaddTempExp.uses(logMakeCoaddTempExp, link=peg.Link.OUTPUT)

            deepCoadd_tempExp = getDataFile(mapper, "deepCoadd_tempExp", coaddTempExpId, create=True)
            dax.addFile(deepCoadd_tempExp)
            makeCoaddTempExp.uses(deepCoadd_tempExp, link=peg.Link.OUTPUT)
            coaddTempExpList.append(deepCoadd_tempExp)

            dax.addJob(makeCoaddTempExp)

        # Pipeline: assembleCoadd per filter
        assembleCoadd = peg.Job(name="assembleCoadd")
        assembleCoadd.uses(mapperFile, link=peg.Link.INPUT)
        assembleCoadd.uses(registry, link=peg.Link.INPUT)
        assembleCoadd.uses(skyMap, link=peg.Link.INPUT)
        assembleCoadd.addArguments(
            outPath, "--output", outPath, ident, " --doraise",
            " ".join(data.id("--selectId") for data in allData[filterName])
        )
        logger.debug(
            "Adding assembleCoadd %s %s %s %s %s %s",
            outPath, "--output", outPath, ident, " --doraise",
            " ".join(data.id("--selectId") for data in allData[filterName])
        )

        # calexp_md is used in SelectDataIdContainer
        for data in allData[filterName]:
            calexp = getDataFile(mapper, "calexp", data.dataId, create=False)
            assembleCoadd.uses(calexp, link=peg.Link.INPUT)

        for coaddTempExp in coaddTempExpList:
            assembleCoadd.uses(coaddTempExp, link=peg.Link.INPUT)

        coaddId = dict(filter=filterName, **patchDataId)
        logAssembleCoadd = peg.File("logAssembleCoadd.%(tract)d-%(patch)s-%(filter)s" % coaddId)
        dax.addFile(logAssembleCoadd)
        assembleCoadd.setStderr(logAssembleCoadd)
        assembleCoadd.uses(logAssembleCoadd, link=peg.Link.OUTPUT)

        coadd = getDataFile(mapper, "deepCoadd", coaddId, create=True)
        dax.addFile(coadd)
        assembleCoadd.uses(coadd, link=peg.Link.OUTPUT)
        dax.addJob(assembleCoadd)

        # Pipeline: detectCoaddSources each coadd (per filter)
        detectCoaddSources = peg.Job(name="detectCoaddSources")
        detectCoaddSources.uses(mapperFile, link=peg.Link.INPUT)
        detectCoaddSources.uses(coadd, link=peg.Link.INPUT)
        detectCoaddSources.addArguments(outPath, "--output", outPath, ident, " --doraise")

        logDetectCoaddSources = peg.File(
            "logDetectCoaddSources.%(tract)d-%(patch)s-%(filter)s" % coaddId)
        dax.addFile(logDetectCoaddSources)
        detectCoaddSources.setStderr(logDetectCoaddSources)
        detectCoaddSources.uses(logDetectCoaddSources, link=peg.Link.OUTPUT)

        for outputType in ["deepCoadd_calexp", "deepCoadd_calexp_background", "deepCoadd_det", "deepCoadd_det_schema"]:
            outFile = getDataFile(mapper, outputType, coaddId, create=True)
            if not dax.hasFile(outFile):  # Only one deepCoadd_det_schema (TODO)
                dax.addFile(outFile)
            detectCoaddSources.uses(outFile, link=peg.Link.OUTPUT)

        dax.addJob(detectCoaddSources)

    # Pipeline: mergeCoaddDetections
    mergeCoaddDetections = peg.Job(name="mergeCoaddDetections")
    mergeCoaddDetections.uses(mapperFile, link=peg.Link.INPUT)
    mergeCoaddDetections.uses(skyMap, link=peg.Link.INPUT)
    inFile = getDataFile(mapper, "deepCoadd_det_schema", patchDataId, create=False)
    mergeCoaddDetections.uses(inFile, link=peg.Link.INPUT)
    for filterName in allExposures:
        coaddId = dict(filter=filterName, **patchDataId)
        inFile = getDataFile(mapper, "deepCoadd_det", coaddId, create=False)
        mergeCoaddDetections.uses(inFile, link=peg.Link.INPUT)

    mergeCoaddDetections.addArguments(
        outPath, "--output", outPath, " --doraise",
        " --id " + patchId + " filter=" + '^'.join(allExposures.keys())
    )

    logMergeCoaddDetections = peg.File("logMergeCoaddDetections.%(tract)d-%(patch)s" % patchDataId)
    dax.addFile(logMergeCoaddDetections)
    mergeCoaddDetections.setStderr(logMergeCoaddDetections)
    mergeCoaddDetections.uses(logMergeCoaddDetections, link=peg.Link.OUTPUT)

    for outputType in ["deepCoadd_mergeDet", "deepCoadd_mergeDet_schema", "deepCoadd_peak_schema"]:
        outFile = getDataFile(mapper, outputType, patchDataId, create=True)
        dax.addFile(outFile)
        mergeCoaddDetections.uses(outFile, link=peg.Link.OUTPUT)

    dax.addJob(mergeCoaddDetections)

    # Pipeline: measureCoaddSources for each filter
    for filterName in allExposures:
        measureCoaddSources = peg.Job(name="measureCoaddSources")
        measureCoaddSources.uses(mapperFile, link=peg.Link.INPUT)
        measureCoaddSources.uses(registry, link=peg.Link.INPUT)
        measureCoaddSources.uses(skyMap, link=peg.Link.INPUT)
        for inputType in ["deepCoadd_mergeDet", "deepCoadd_mergeDet_schema", "deepCoadd_peak_schema"]:
            inFile = getDataFile(mapper, inputType, patchDataId, create=False)
            measureCoaddSources.uses(inFile, link=peg.Link.INPUT)

        coaddId = dict(filter=filterName, **patchDataId)
        for inputType in ["deepCoadd_calexp"]:
            inFile = getDataFile(mapper, inputType, coaddId, create=False)
            measureCoaddSources.uses(inFile, link=peg.Link.INPUT)

        # src is used in the PropagateVisitFlagsTask subtask
        for data in allData[filterName]:
            src = getDataFile(mapper, "src", data.dataId, create=False)
            measureCoaddSources.uses(src, link=peg.Link.INPUT)

        measureCoaddSources.addArguments(
            outPath, "--output", outPath, " --doraise",
            " --id " + patchId + " filter=" + filterName
        )
        logMeasureCoaddSources = peg.File(
            "logMeasureCoaddSources.%(tract)d-%(patch)s-%(filter)s" % coaddId)
        dax.addFile(logMeasureCoaddSources)
        measureCoaddSources.setStderr(logMeasureCoaddSources)
        measureCoaddSources.uses(logMeasureCoaddSources, link=peg.Link.OUTPUT)

        for outputType in ["deepCoadd_meas_schema", "deepCoadd_meas", "deepCoadd_srcMatch"]:
            outFile = getDataFile(mapper, outputType, coaddId, create=True)
            if not dax.hasFile(outFile):  # Only one deepCoadd_meas_schema (TODO)
                dax.addFile(outFile)
            measureCoaddSources.uses(outFile, link=peg.Link.OUTPUT)

        dax.addJob(measureCoaddSources)

    # Pipeline: mergeCoaddMeasurements
    mergeCoaddMeasurements = peg.Job(name="mergeCoaddMeasurements")
    mergeCoaddMeasurements.uses(mapperFile, link=peg.Link.INPUT)
    inFile = getDataFile(mapper, "deepCoadd_meas_schema", patchDataId, create=False)
    mergeCoaddMeasurements.uses(inFile, link=peg.Link.INPUT)
    for filterName in allExposures:
        coaddId = dict(filter=filterName, **patchDataId)
        inFile = getDataFile(mapper, "deepCoadd_meas", coaddId, create=False)
        mergeCoaddMeasurements.uses(inFile, link=peg.Link.INPUT)

    mergeCoaddMeasurements.addArguments(
        outPath, "--output", outPath, " --doraise",
        " --id " + patchId + " filter=" + '^'.join(allExposures.keys())
    )

    logMergeCoaddMeasurements = peg.File(
        "logMergeCoaddMeasurements.%(tract)d-%(patch)s" % patchDataId)
    dax.addFile(logMergeCoaddMeasurements)
    mergeCoaddMeasurements.setStderr(logMergeCoaddMeasurements)
    mergeCoaddMeasurements.uses(logMergeCoaddMeasurements, link=peg.Link.OUTPUT)

    for outputType in ["deepCoadd_ref", "deepCoadd_ref_schema"]:
        outFile = getDataFile(mapper, outputType, patchDataId, create=True)
        dax.addFile(outFile)
        mergeCoaddMeasurements.uses(outFile, link=peg.Link.OUTPUT)

    dax.addJob(mergeCoaddMeasurements)

    # Pipeline: forcedPhotCoadd for each filter
    for filterName in allExposures:
        forcedPhotCoadd = peg.Job(name="forcedPhotCoadd")
        forcedPhotCoadd.uses(mapperFile, link=peg.Link.INPUT)
        forcedPhotCoadd.uses(skyMap, link=peg.Link.INPUT)
        for inputType in ["deepCoadd_ref_schema", "deepCoadd_ref"]:
            inFile = getDataFile(mapper, inputType, patchDataId, create=False)
            forcedPhotCoadd.uses(inFile, link=peg.Link.INPUT)

        coaddId = dict(filter=filterName, **patchDataId)
        for inputType in ["deepCoadd_calexp", "deepCoadd_meas"]:
            inFile = getDataFile(mapper, inputType, coaddId, create=False)
            forcedPhotCoadd.uses(inFile, link=peg.Link.INPUT)

        forcedPhotCoadd.addArguments(
            outPath, "--output", outPath, " --doraise",
            " --id " + patchId + " filter=" + filterName
        )

        logForcedPhotCoadd = peg.File("logForcedPhotCoadd.%(tract)d-%(patch)s-%(filter)s" % coaddId)
        dax.addFile(logForcedPhotCoadd)
        forcedPhotCoadd.setStderr(logForcedPhotCoadd)
        forcedPhotCoadd.uses(logForcedPhotCoadd, link=peg.Link.OUTPUT)

        for outputType in ["deepCoadd_forced_src_schema", "deepCoadd_forced_src"]:
            outFile = getDataFile(mapper, outputType, coaddId, create=True)
            if not dax.hasFile(outFile):  # Only one deepCoadd_forced_src_schema (TODO)
                dax.addFile(outFile)
            forcedPhotCoadd.uses(outFile, link=peg.Link.OUTPUT)

        dax.addJob(forcedPhotCoadd)

    # Pipeline: forcedPhotCcd for each ccd

    for data in sum(allData.itervalues(), []):
        forcedPhotCcd = peg.Job(name="forcedPhotCcd")
        forcedPhotCcd.uses(mapperFile, link=peg.Link.INPUT)
        forcedPhotCcd.uses(registry, link=peg.Link.INPUT)
        forcedPhotCcd.uses(skyMap, link=peg.Link.INPUT)
        calexp = getDataFile(mapper, "calexp", data.dataId, create=False)
        forcedPhotCcd.uses(calexp, link=peg.Link.INPUT)
        for inputType in ["deepCoadd_ref_schema", "deepCoadd_ref"]:
            inFile = getDataFile(mapper, inputType, patchDataId, create=False)
            forcedPhotCcd.uses(inFile, link=peg.Link.INPUT)

        forcedPhotCcd.uses(forcedPhotCcdConfig, link=peg.Link.INPUT)
        forcedPhotCcd.addArguments(outPath, "--output", outPath, " --doraise",
                                   "-C", forcedPhotCcdConfig, data.id(tract=0))
        logger.debug("forcedPhotCcd with %s", data.id(tract=0))

        logForcedPhotCcd = peg.File("logForcedPhotCcd.%s" % data.name)
        dax.addFile(logForcedPhotCcd)
        forcedPhotCcd.setStderr(logForcedPhotCcd)
        forcedPhotCcd.uses(logForcedPhotCcd, link=peg.Link.OUTPUT)

        for outputType in ["forced_src", "forced_src_schema"]:
            dataId = dict(tract=0, **data.dataId)
            outFile = getDataFile(mapper, outputType, dataId, create=True)
            if not dax.hasFile(outFile):  # Only one forced_src_schema (TODO)
                dax.addFile(outFile)
            forcedPhotCcd.uses(outFile, link=peg.Link.OUTPUT)

        dax.addJob(forcedPhotCcd)

    return dax


if __name__ == "__main__":
    dax = generateDax("CiHscDax")
    f = open("ciHsc.dax", "w")
    dax.writeXML(f)
    f.close()
