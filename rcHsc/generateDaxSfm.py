#!/usr/bin/env python
import argparse
import os
import Pegasus.DAX3 as peg

import lsst.log
import lsst.utils
from lsst.utils import getPackageDir
from lsst.daf.persistence import Butler
from lsst.obs.hsc.hscMapper import HscMapper
from findShardId import findShardIdFromExpId
from getDataFile import getDataFile

logger = lsst.log.Log.getLogger("workflow")
logger.setLevel(lsst.log.INFO)

# hard-coded output repo
# A local output repo is written when running this script;
# this local repo is not used at all for actual job submission and run.
# Real submitted run dumps output in scratch (specified in the site catalog).
outPath = 'repo'
logger.debug("outPath: %s", outPath)

inputRepo = "/datasets/hsc/repo"
calibRepo = "/datasets/hsc/repo/CALIB/"

# This is a config of LoadIndexedReferenceObjectsTask ref_dataset_name
refcatName = "ps1_pv3_3pi_20170110"


def generateSfmDax(name="dax", visits=None, ccdList=None):
    """Generate a Pegasus DAX abstract workflow"""
    try:
        from AutoADAG import AutoADAG
    except ImportError:
        dax = peg.ADAG(name)
    else:
        dax = AutoADAG(name)

    # Construct these mappers only for creating dax, not for actual runs.
    mapper = HscMapper(root=inputRepo, calibRoot=calibRepo)

    # Get the following butler or config files directly from ci_hsc package
    filePathMapper = os.path.join(inputRepo, "_mapper")
    mapperFile = peg.File(os.path.join(outPath, "_mapper"))
    mapperFile.addPFN(peg.PFN(filePathMapper, site="local"))
    mapperFile.addPFN(peg.PFN(filePathMapper, site="lsstvc"))
    dax.addFile(mapperFile)

    filePathRegistry = os.path.join(inputRepo, "registry.sqlite3")
    registry = peg.File(os.path.join(outPath, "registry.sqlite3"))
    registry.addPFN(peg.PFN(filePathRegistry, site="local"))
    registry.addPFN(peg.PFN(filePathRegistry, site="lsstvc"))
    dax.addFile(registry)

    filePathCalibRegistry = os.path.join(calibRepo, "calibRegistry.sqlite3")
    calibRegistry = peg.File(os.path.join(outPath, "calibRegistry.sqlite3"))
    calibRegistry.addPFN(peg.PFN(filePathCalibRegistry, site="local"))
    calibRegistry.addPFN(peg.PFN(filePathCalibRegistry, site="lsstvc"))
    dax.addFile(calibRegistry)

    # Add necessities for ref_cats
    refCatConfigFile = getDataFile(mapper, "ref_cat_config", {"name": refcatName}, create=True, repoRoot=inputRepo)
    dax.addFile(refCatConfigFile)

    refCatSchema = "ref_cats/ps1_pv3_3pi_20170110/master_schema.fits"
    filePath = os.path.join(inputRepo, refCatSchema)
    refCatSchemaFile = peg.File(os.path.join(outPath, refCatSchema))
    refCatSchemaFile.addPFN(peg.PFN(filePath, site="local"))
    refCatSchemaFile.addPFN(peg.PFN(filePath, site="lsstvc"))
    dax.addFile(refCatSchemaFile)

    # Hack to set fringe filters
    # Cannot directly read from obs_subaru/config/hsc/isr.py because
    # config uses abbrev names config.fringe.filters = ['y', 'N921']
    fringeFilters = ["HSC-Y", "NB0921"]

    # Add prerun
    preProcessCcd = peg.Job(name="processCcd")
    preProcessCcd.uses(mapperFile, link=peg.Link.INPUT)
    preProcessCcd.uses(refCatConfigFile, link=peg.Link.INPUT)
    preProcessCcd.addArguments(outPath, "--output", outPath, " --doraise")
    for schema in ["icSrc_schema", "src_schema"]:
        outFile = getDataFile(mapper, schema, {}, create=True)
        dax.addFile(outFile)
        preProcessCcd.uses(outFile, link=peg.Link.OUTPUT)
    dax.addJob(preProcessCcd)

    # Pipeline: processCcd
    for visit in visits:
        for ccd in ccdList:
            dataId = {'visit': int(visit), 'ccd': ccd}
            logger.debug("processCcd dataId: %s", dataId)

            processCcd = peg.Job(name="processCcd")
            processCcd.addArguments(outPath, "--calib", outPath, "--output", outPath,
                                    " --doraise --id visit={visit} ccd={ccd}".format(**dataId))
            processCcd.uses(registry, link=peg.Link.INPUT)
            processCcd.uses(calibRegistry, link=peg.Link.INPUT)
            processCcd.uses(mapperFile, link=peg.Link.INPUT)
            for inputType in ["icSrc_schema", "src_schema"]:
                inFile = getDataFile(mapper, inputType, {}, create=False)
                processCcd.uses(inFile, link=peg.Link.INPUT)

            processCcd.uses(refCatConfigFile, link=peg.Link.INPUT)
            processCcd.uses(refCatSchemaFile, link=peg.Link.INPUT)

            inFile = getDataFile(mapper, "raw", dataId, create=True, repoRoot=inputRepo)
            dax.addFile(inFile)
            processCcd.uses(inFile, link=peg.Link.INPUT)
            for inputType in ["bias", "dark", "flat", "bfKernel"]:
                inFile = getDataFile(mapper, inputType, dataId,
                                     create=True, repoRoot=calibRepo)
                if not dax.hasFile(inFile):
                    dax.addFile(inFile)
                processCcd.uses(inFile, link=peg.Link.INPUT)

            filterName = mapper.queryMetadata(datasetType="raw", format=("filter",), dataId={'visit':visit})[0][0]
            if filterName in fringeFilters:
                inFile = getDataFile(mapper, "fringe", dataId,
                                     create=True, repoRoot=calibRepo)
                if not dax.hasFile(inFile):
                    dax.addFile(inFile)
                processCcd.uses(inFile, link=peg.Link.INPUT)

            for outputType in ["calexp", "src", "srcMatch"]:
                outFile = getDataFile(mapper, outputType, dataId, create=True)
                dax.addFile(outFile)
                processCcd.uses(outFile, link=peg.Link.OUTPUT)

            butler = Butler(root=inputRepo, calibRoot=calibRepo)
            shards = findShardIdFromExpId(butler, dataId)
            for shard in shards:
                refCatFile = getDataFile(mapper, "ref_cat", {"name": refcatName, "pixel_id": shard}, create=True, repoRoot=inputRepo)
                if not dax.hasFile(refCatFile):
                    dax.addFile(refCatFile)
                    logger.info("Add ref_cat file %s" % refCatFile)
                processCcd.uses(refCatFile, link=peg.Link.INPUT)

            logProcessCcd = peg.File("logProcessCcd.v{visit}.c{ccd}".format(**dataId))
            dax.addFile(logProcessCcd)
            processCcd.setStderr(logProcessCcd)
            processCcd.uses(logProcessCcd, link=peg.Link.OUTPUT)

            dax.addJob(processCcd)

    # Pipeline: makeSkyMap
    makeSkyMap = peg.Job(name="makeSkyMap")
    makeSkyMap.uses(mapperFile, link=peg.Link.INPUT)
    makeSkyMap.addArguments(outPath, "--output", outPath, " --doraise")
    logMakeSkyMap = peg.File("logMakeSkyMap")
    dax.addFile(logMakeSkyMap)
    makeSkyMap.setStderr(logMakeSkyMap)
    makeSkyMap.uses(logMakeSkyMap, link=peg.Link.OUTPUT)

    skyMap = getDataFile(mapper, "deepCoadd_skyMap", {}, create=True)
    dax.addFile(skyMap)
    makeSkyMap.uses(skyMap, link=peg.Link.OUTPUT)

    dax.addJob(makeSkyMap)

    return dax


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a DAX")
    parser.add_argument("-i", "--inputData", default="rcHsc/visitsRcTest.txt",
                        help="a file including input data information")
    parser.add_argument("-o", "--outputFile", type=str, default="HscRcTest.dax",
                        help="file name for the output dax xml")
    args = parser.parse_args()
    with open(args.inputData) as f:
        visits = [line.rstrip() for line in f]

    ccdList = range(9) + range(10, 104)
    dax = generateSfmDax("HscSfmDax", visits, ccdList)
    with open(args.outputFile, "w") as f:
        dax.writeXML(f)
