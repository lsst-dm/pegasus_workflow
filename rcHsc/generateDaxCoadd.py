#!/usr/bin/env python
import argparse
import os
import Pegasus.DAX3 as peg

import lsst.log
import lsst.utils
from lsst.utils import getPackageDir
from lsst.daf.persistence import Butler
from lsst.obs.hsc.hscMapper import HscMapper
from findShardId import findShardIdFromPatch
from getDataFile import getDataFile

logger = lsst.log.Log.getLogger("workflow")
logger.setLevel(lsst.log.WARN)

# hard-coded output repo
# A local output repo is written when running this script;
# this local repo is not used at all for actual job submission and run.
# Real submitted run dumps output in scratch (specified in the site catalog).
# This hack is also hard-coded in getDataFile.py
outPath = 'repo'
logger.debug("outPath: %s", outPath)

inputRepo = "/project/hsc_rc/w_2017_28/DM-11184/"
rootRepo = "/datasets/hsc/repo"

# This is a config of LoadIndexedReferenceObjectsTask ref_dataset_name
refcatName = "ps1_pv3_3pi_20170110"


def generateCoaddDax(name="dax", tractDataId=0, dataDict=None, blacklist=None, doMosaic=False):
    """Generate a Pegasus DAX abstract workflow"""
    try:
        from AutoADAG import AutoADAG
    except ImportError:
        dax = peg.ADAG(name)
    else:
        dax = AutoADAG(name)

    # Construct these mappers only for creating dax, not for actual runs.
    mapper = HscMapper(root=rootRepo)

    # Construct a butler only for finding ref cat shards
    butler = Butler(inputRepo)

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

    # schema from processCcd
    srcSchema = getDataFile(mapper, "src_schema", {}, create=True, repoRoot=inputRepo)
    dax.addFile(srcSchema)

    # pre-run detectCoaddSources for schema
    preDetectCoaddSources = peg.Job(name="detectCoaddSources")
    preDetectCoaddSources.uses(mapperFile, link=peg.Link.INPUT)
    preDetectCoaddSources.addArguments(outPath, "--output", outPath, " --doraise")
    deepCoadd_det_schema = getDataFile(mapper, "deepCoadd_det_schema", {}, create=True)
    dax.addFile(deepCoadd_det_schema)
    preDetectCoaddSources.uses(deepCoadd_det_schema, link=peg.Link.OUTPUT)
    dax.addJob(preDetectCoaddSources)

    # pre-run mergeCoaddDetections
    preMergeCoaddDetections = peg.Job(name="mergeCoaddDetections")
    preMergeCoaddDetections.uses(mapperFile, link=peg.Link.INPUT)
    preMergeCoaddDetections.uses(deepCoadd_det_schema, link=peg.Link.INPUT)
    preMergeCoaddDetections.addArguments(outPath, "--output", outPath, " --doraise")
    for schema in ["deepCoadd_mergeDet_schema", "deepCoadd_peak_schema"]:
        outFile = getDataFile(mapper, schema, {}, create=True)
        dax.addFile(outFile)
        preMergeCoaddDetections.uses(outFile, link=peg.Link.OUTPUT)
    dax.addJob(preMergeCoaddDetections)

    # pre-run measureCoaddSources
    preMeasureCoaddSources = peg.Job(name="measureCoaddSources")
    preMeasureCoaddSources.uses(mapperFile, link=peg.Link.INPUT)
    preMeasureCoaddSources.uses(refCatConfigFile, link=peg.Link.INPUT)
    preMeasureCoaddSources.addArguments(outPath, "--output", outPath, " --doraise")
    for inputType in ["deepCoadd_mergeDet_schema", "deepCoadd_peak_schema", "src_schema"]:
        inFile = getDataFile(mapper, inputType, {}, create=False)
        preMeasureCoaddSources.uses(inFile, link=peg.Link.INPUT)

    deepCoadd_meas_schema = getDataFile(mapper, "deepCoadd_meas_schema", {}, create=True)
    dax.addFile(deepCoadd_meas_schema)
    preMeasureCoaddSources.uses(deepCoadd_meas_schema, link=peg.Link.OUTPUT)
    dax.addJob(preMeasureCoaddSources)

    # pre-run mergeCoaddMeasurements
    preMergeCoaddMeasurements = peg.Job(name="mergeCoaddMeasurements")
    preMergeCoaddMeasurements.uses(mapperFile, link=peg.Link.INPUT)
    preMergeCoaddMeasurements.uses(deepCoadd_meas_schema, link=peg.Link.INPUT)
    preMergeCoaddMeasurements.addArguments(outPath, "--output", outPath, " --doraise")
    deepCoadd_ref_schema = getDataFile(mapper, "deepCoadd_ref_schema", {}, create=True)
    dax.addFile(deepCoadd_ref_schema)
    preMergeCoaddMeasurements.uses(deepCoadd_ref_schema, link=peg.Link.OUTPUT)
    dax.addJob(preMergeCoaddMeasurements)

    # pre-run forcedPhotCoadd
    preForcedPhotCoadd = peg.Job(name="forcedPhotCoadd")
    preForcedPhotCoadd.uses(mapperFile, link=peg.Link.INPUT)
    preForcedPhotCoadd.uses(deepCoadd_ref_schema, link=peg.Link.INPUT)
    preForcedPhotCoadd.addArguments(outPath, "--output", outPath, " --doraise")
    deepCoadd_forced_src_schema = getDataFile(mapper, "deepCoadd_forced_src_schema", {}, create=True)
    dax.addFile(deepCoadd_forced_src_schema)
    preForcedPhotCoadd.uses(deepCoadd_forced_src_schema, link=peg.Link.OUTPUT)
    dax.addJob(preForcedPhotCoadd)

    # workaround for DM-10634
    filePath = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            "safeClipAssembleCoaddConfig.py")
    coaddConfig = peg.File("safeClipAssembleCoaddConfig.py")
    coaddConfig.addPFN(peg.PFN(filePath, site="lsstvc"))
    dax.addFile(coaddConfig)

    # Pipeline: processCcd  -- DONE previously
    # Pipeline: makeSkyMap  -- DONE previously
    skyMap = getDataFile(mapper, "deepCoadd_skyMap", {}, create=True, repoRoot=inputRepo)
    dax.addFile(skyMap)

    # Pipeline: mosaic per filter
    if doMosaic:
        for filterName in dataDict:
            visits = set()
            # this ccds needs to consider backlist where no sfm results are available
            ccds = range(9) + range(10, 104)
            for patchDataId in dataDict[filterName]:
                for ccd in dataDict[filterName][patchDataId]:
                    visitId, ccdId = map(int, ccd.split('-'))
                    visits.add(visitId)
                    #ccds.add(ccd)

            #visits = map(int, {ccd.split('-')[0] for ccd in dataDict[filterName][patch] for patch in dataDict[filterName]})

            mosaic = peg.Job(name="mosaic")
            mosaic.uses(mapperFile, link=peg.Link.INPUT)
            mosaic.uses(registry, link=peg.Link.INPUT)
            mosaic.uses(skyMap, link=peg.Link.INPUT)
            mosaic.uses(refCatConfigFile, link=peg.Link.INPUT)
            mosaic.uses(refCatSchemaFile, link=peg.Link.INPUT)
            mosaic.uses(srcSchema, link=peg.Link.INPUT)
            for visitId in visits:
                for ccdId in ccds:
                #visitId, ccdId = map(int, ccd.split('-'))
                    ccdStr = "%d-%d" % (visitId, ccdId)
                    if ccdStr in blacklist:
                        continue
                    for inputType in ["calexp", "src", "srcMatch"]:
                        inFile = getDataFile(mapper, inputType, {'visit': visitId, 'ccd': ccdId},
                                             create=True, repoRoot=inputRepo)
                        if not dax.hasFile(inFile):
                            dax.addFile(inFile)
                        mosaic.uses(inFile, link=peg.Link.INPUT)
                    for outputType in ["wcs", "fcr"]:
                        outFile = getDataFile(mapper, outputType,
                                      {'visit': visitId, 'ccd': ccdId, 'tract': tractDataId},
                                      create=True)
                        dax.addFile(outFile)
                        mosaic.uses(outFile, link=peg.Link.OUTPUT)

            # MosaicTask uses meas.algorithsm.LoadReferenceObjectsTask.joinMatchListWithCatalog
            # Here I use skymap patches instead, so not to read catalogs
            refs = set()
            for patchDataId in dataDict[filterName]:
                tractPatchDataId = dict(tract=tractDataId, patch=patchDataId)
                shards = findShardIdFromPatch(butler, tractPatchDataId)
                for shard in shards:
                    refCatFile = getDataFile(mapper, "ref_cat", {"name": refcatName, "pixel_id": shard}, create=True, repoRoot=rootRepo)
                    if not dax.hasFile(refCatFile):
                        dax.addFile(refCatFile)
                        logger.debug("Add ref_cat file %s" % refCatFile)
                    if refCatFile not in refs:
                        mosaic.uses(refCatFile, link=peg.Link.INPUT)
                        refs.add(refCatFile)

            mosaic.addArguments(
                outPath, "--output", outPath, " --doraise",
                " --id ccd=0..8^10..103 visit="+"^".join(str(visit) for visit in visits),
                " tract=%s" % tractDataId,
                #" --diagnostics --diagDir=%s" % TBD
            )

            logMosaic = peg.File("logMosaic.%d-%s" % (tractDataId, filterName))
            dax.addFile(logMosaic)
            mosaic.setStderr(logMosaic)
            mosaic.setStdout(logMosaic)
            mosaic.uses(logMosaic, link=peg.Link.OUTPUT)
            dax.addJob(mosaic)


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

                if doMosaic:
                    applyMosaic = " "
                    for ccdId in visitDict[visitId]:
                        for inputType in ["wcs", "fcr"]:
                            inFile = getDataFile(mapper, inputType,
                                         {'visit': visitId, 'ccd': ccdId, 'tract': tractDataId},
                                         create=False)
                            makeCoaddTempExp.uses(inFile, link=peg.Link.INPUT)
                else:
                    applyMosaic = " -c doApplyUberCal=False "

                makeCoaddTempExp.addArguments(
                    outPath, "--output", outPath, " --doraise",
                    ident, applyMosaic,
                    "--selectId visit=%s ccd=%s" % (visitId, '^'.join(str(ccdId) for ccdId in visitDict[visitId]))
                )
                logger.debug(
                    "Adding makeCoaddTempExp %s %s %s %s %s %s %s",
                    outPath, "--output", outPath, " --doraise",
                    ident, applyMosaic,
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
            # workaround for DM-10634
            assembleCoadd.uses(coaddConfig, link=peg.Link.INPUT)
            assembleCoadd.addArguments(
                outPath, "--output", outPath, ident, " --doraise",
                "-C", coaddConfig,
                " --selectId visit=" + " --selectId visit=".join(str(visitId) for visitId in visitDict)
            )
            logger.debug(
                "Adding assembleCoadd %s %s %s %s %s %s",
                outPath, "--output", outPath, ident, " --doraise",
                "-C", coaddConfig,
                " --selectId visit=" + " --selectId visit=".join(str(visitId) for visitId in visitDict)
            )

            # calexp_md is used in SelectDataIdContainer
            # src is used in PsfWcsSelectImagesTask
            assembleCoadd.uses(srcSchema, link=peg.Link.INPUT)
            for visitId in visitDict:
                for ccdId in visitDict[visitId]:
                    for inputType in ["calexp", "src"]:
                        inFile = getDataFile(mapper, inputType, {'visit': visitId, 'ccd': ccdId},
                                             create=True, repoRoot=inputRepo)
                        if not dax.hasFile(inFile):
                            dax.addFile(inFile)
                        assembleCoadd.uses(inFile, link=peg.Link.INPUT)

            for coaddTempExp in coaddTempExpList:
                assembleCoadd.uses(coaddTempExp, link=peg.Link.INPUT)

            coaddId = dict(filter=filterName, tract=tractDataId, patch=patchDataId)
            brightObjectMask = getDataFile(mapper, "brightObjectMask", coaddId,
                                           create=True, repoRoot=rootRepo)
            dax.addFile(brightObjectMask)
            assembleCoadd.uses(brightObjectMask, link=peg.Link.INPUT)

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

    allPatches = list({patch for patch in dataDict[filterName] for filterName in dataDict})

    # Pipeline: mergeCoaddDetections per patch
    for patchDataId in allPatches:
        tractPatchDataId = dict(tract=tractDataId, patch=patchDataId)
        ident = "--id " + " ".join(("%s=%s" % (k, v) for k, v in tractPatchDataId.iteritems()))
        mergeCoaddDetections = peg.Job(name="mergeCoaddDetections")
        mergeCoaddDetections.uses(mapperFile, link=peg.Link.INPUT)
        mergeCoaddDetections.uses(skyMap, link=peg.Link.INPUT)
        inFile = getDataFile(mapper, "deepCoadd_det_schema", tractPatchDataId, create=False)
        mergeCoaddDetections.uses(inFile, link=peg.Link.INPUT)
        for filterName in dataDict:
            coaddId = dict(filter=filterName, **tractPatchDataId)
            inFile = getDataFile(mapper, "deepCoadd_det", coaddId, create=False)
            mergeCoaddDetections.uses(inFile, link=peg.Link.INPUT)

        mergeCoaddDetections.addArguments(
            outPath, "--output", outPath, " --doraise",
            ident + " filter=" + '^'.join(dataDict.keys())
        )

        logMergeCoaddDetections = peg.File("logMergeCoaddDetections.%(tract)d-%(patch)s" % tractPatchDataId)
        dax.addFile(logMergeCoaddDetections)
        mergeCoaddDetections.setStderr(logMergeCoaddDetections)
        mergeCoaddDetections.uses(logMergeCoaddDetections, link=peg.Link.OUTPUT)

        for inputType in ["deepCoadd_mergeDet_schema", "deepCoadd_peak_schema"]:
            inFile = getDataFile(mapper, inputType, {}, create=False)
            mergeCoaddDetections.uses(inFile, link=peg.Link.INPUT)
        for outputType in ["deepCoadd_mergeDet"]:
            outFile = getDataFile(mapper, outputType, tractPatchDataId, create=True)
            dax.addFile(outFile)
            mergeCoaddDetections.uses(outFile, link=peg.Link.OUTPUT)

        dax.addJob(mergeCoaddDetections)

    # Pipeline: measureCoaddSources per filter per patch
    for filterName in dataDict:
        for patchDataId in dataDict[filterName]:
            tractPatchDataId = dict(tract=tractDataId, patch=patchDataId)
            coaddId = dict(filter=filterName, **tractPatchDataId)
            ident = "--id " + " ".join(("%s=%s" % (k, v) for k, v in coaddId.iteritems()))

            measureCoaddSources = peg.Job(name="measureCoaddSources")
            measureCoaddSources.uses(mapperFile, link=peg.Link.INPUT)
            measureCoaddSources.uses(registry, link=peg.Link.INPUT)
            measureCoaddSources.uses(skyMap, link=peg.Link.INPUT)
            measureCoaddSources.uses(refCatConfigFile, link=peg.Link.INPUT)
            measureCoaddSources.uses(refCatSchemaFile, link=peg.Link.INPUT)

            # The pipeline uses the source catalog to decide what ref shards to need
            # Here I use skymap patches instead, so not to read source catalog
            shards = findShardIdFromPatch(butler, tractPatchDataId)
            for shard in shards:
                refCatFile = getDataFile(mapper, "ref_cat", {"name": refcatName, "pixel_id": shard}, create=True, repoRoot=rootRepo)
                if not dax.hasFile(refCatFile):
                    dax.addFile(refCatFile)
                    logger.debug("Add ref_cat file %s" % refCatFile)
                measureCoaddSources.uses(refCatFile, link=peg.Link.INPUT)

            for inputType in ["deepCoadd_mergeDet", "deepCoadd_mergeDet_schema", "deepCoadd_peak_schema"]:
                inFile = getDataFile(mapper, inputType, tractPatchDataId, create=False)
                measureCoaddSources.uses(inFile, link=peg.Link.INPUT)

            for inputType in ["deepCoadd_calexp"]:
                inFile = getDataFile(mapper, inputType, coaddId, create=False)
                measureCoaddSources.uses(inFile, link=peg.Link.INPUT)

            # src is used in the PropagateVisitFlagsTask subtask
            measureCoaddSources.uses(srcSchema, link=peg.Link.INPUT)
            for ccd in dataDict[filterName][patchDataId]:
                visitId, ccdId = map(int, ccd.split('-'))
                src = getDataFile(mapper, "src", {'visit': visitId, 'ccd': ccdId},
                                  create=False)
                measureCoaddSources.uses(src, link=peg.Link.INPUT)

            measureCoaddSources.addArguments(
                outPath, "--output", outPath, " --doraise", ident
            )
            logMeasureCoaddSources = peg.File(
                "logMeasureCoaddSources.%(tract)d-%(patch)s-%(filter)s" % coaddId)
            dax.addFile(logMeasureCoaddSources)
            measureCoaddSources.setStderr(logMeasureCoaddSources)
            measureCoaddSources.uses(logMeasureCoaddSources, link=peg.Link.OUTPUT)

            inFile = getDataFile(mapper, "deepCoadd_meas_schema", {}, create=False)
            measureCoaddSources.uses(inFile, link=peg.Link.INPUT)
            for outputType in ["deepCoadd_meas", "deepCoadd_measMatch"]:
                outFile = getDataFile(mapper, outputType, coaddId, create=True)
                dax.addFile(outFile)
                measureCoaddSources.uses(outFile, link=peg.Link.OUTPUT)

            dax.addJob(measureCoaddSources)

    # Pipeline: mergeCoaddMeasurements per patch
    for patchDataId in allPatches:
        tractPatchDataId = dict(tract=tractDataId, patch=patchDataId)
        ident = "--id " + " ".join(("%s=%s" % (k, v) for k, v in tractPatchDataId.iteritems()))
        mergeCoaddMeasurements = peg.Job(name="mergeCoaddMeasurements")
        mergeCoaddMeasurements.uses(mapperFile, link=peg.Link.INPUT)
        inFile = getDataFile(mapper, "deepCoadd_meas_schema", tractPatchDataId, create=False)
        mergeCoaddMeasurements.uses(inFile, link=peg.Link.INPUT)
        for filterName in dataDict:
            coaddId = dict(filter=filterName, **tractPatchDataId)
            inFile = getDataFile(mapper, "deepCoadd_meas", coaddId, create=False)
            mergeCoaddMeasurements.uses(inFile, link=peg.Link.INPUT)

        mergeCoaddMeasurements.addArguments(
            outPath, "--output", outPath, " --doraise",
            ident + " filter=" + '^'.join(dataDict.keys())
        )

        logMergeCoaddMeasurements = peg.File(
            "logMergeCoaddMeasurements.%(tract)d-%(patch)s" % tractPatchDataId)
        dax.addFile(logMergeCoaddMeasurements)
        mergeCoaddMeasurements.setStderr(logMergeCoaddMeasurements)
        mergeCoaddMeasurements.uses(logMergeCoaddMeasurements, link=peg.Link.OUTPUT)

        outFile = getDataFile(mapper, "deepCoadd_ref", tractPatchDataId, create=True)
        dax.addFile(outFile)
        mergeCoaddMeasurements.uses(outFile, link=peg.Link.OUTPUT)

        dax.addJob(mergeCoaddMeasurements)

    # Pipeline: forcedPhotCoadd per patch per filter
    for filterName in dataDict:
        for patchDataId in dataDict[filterName]:
            tractPatchDataId = dict(tract=tractDataId, patch=patchDataId)
            coaddId = dict(filter=filterName, **tractPatchDataId)
            ident = "--id " + " ".join(("%s=%s" % (k, v) for k, v in coaddId.iteritems()))

            forcedPhotCoadd = peg.Job(name="forcedPhotCoadd")
            forcedPhotCoadd.uses(mapperFile, link=peg.Link.INPUT)
            forcedPhotCoadd.uses(skyMap, link=peg.Link.INPUT)
            for inputType in ["deepCoadd_ref_schema", "deepCoadd_ref"]:
                inFile = getDataFile(mapper, inputType, tractPatchDataId, create=False)
                forcedPhotCoadd.uses(inFile, link=peg.Link.INPUT)

            for inputType in ["deepCoadd_calexp", "deepCoadd_meas"]:
                inFile = getDataFile(mapper, inputType, coaddId, create=False)
                forcedPhotCoadd.uses(inFile, link=peg.Link.INPUT)

            forcedPhotCoadd.addArguments(
                outPath, "--output", outPath, " --doraise", ident
            )

            logForcedPhotCoadd = peg.File("logForcedPhotCoadd.%(tract)d-%(patch)s-%(filter)s" % coaddId)
            dax.addFile(logForcedPhotCoadd)
            forcedPhotCoadd.setStderr(logForcedPhotCoadd)
            forcedPhotCoadd.uses(logForcedPhotCoadd, link=peg.Link.OUTPUT)

            inFile = getDataFile(mapper, "deepCoadd_forced_src_schema", {}, create=False)
            forcedPhotCoadd.uses(inFile, link=peg.Link.INPUT)
            outFile = getDataFile(mapper, "deepCoadd_forced_src", coaddId, create=True)
            dax.addFile(outFile)
            forcedPhotCoadd.uses(outFile, link=peg.Link.OUTPUT)

            dax.addJob(forcedPhotCoadd)

    return dax


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a DAX")
    parser.add_argument("-t", "--tractId", type=int, default=8766,
                        help="the tract ID of the input file")
    parser.add_argument("-i", "--inputData", default="rcHsc/smallFPVC_t8766",
                        help="a file including input data information")
    parser.add_argument("-b", "--blacklist", default="rcHsc/rcBlacklist.txt",
                        help="a file including visit-ccd to ignore")
    parser.add_argument("-o", "--outputFile", type=str, default="HscRcTest.dax",
                        help="file name for the output dax xml")
    args = parser.parse_args()

    with open(args.blacklist, "r") as f:
        blacklist = [line.rstrip() for line in f]

    from collections import defaultdict
    dataDict = defaultdict(dict)
    # dataDict[filterName][patch] is a list of 'visit-ccd'
    with open(args.inputData, "r") as f:
        for line in f:
            filterName, patchId, visitCcds = line.rstrip().split('|')
            dataDict[filterName][patchId] = [visitCcd for visitCcd in visitCcds.split(',') if visitCcd not in blacklist]

    logger.debug("dataDict: %s", dataDict)
    dax = generateCoaddDax("HscCoaddDax", args.tractId, dataDict, blacklist=blacklist, doMosaic=True)
    with open(args.outputFile, "w") as f:
        dax.writeXML(f)
