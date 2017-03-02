#!/usr/bin/env python
import argparse
import os
import Pegasus.DAX3 as peg

import lsst.log
import lsst.utils
from lsst.utils import getPackageDir
from lsst.obs.hsc.hscMapper import HscMapper

logger = lsst.log.Log.getLogger("workflow")
logger.setLevel(lsst.log.DEBUG)

# hard-coded output repo
# A local output repo is written when running this script;
# this local repo is not used at all for actual job submission and run.
# Real submitted run dumps output in scratch (specified in the site catalog).
outPath = 'repo'
logger.debug("outPath: %s", outPath)

inputRepo = "/datasets/hsc/repo"
calibRepo = "/datasets/hsc/repo/CALIB/20160419"


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
    logger.debug("Finding %s with %s", datasetType, dataId)
    mapFunc = getattr(mapper, "map_" + datasetType)
    fileEntry = lfn = filePath = mapFunc(dataId).getLocations()[0]

    if replaceRootPath is not None:
        lfn = filePath.replace(replaceRootPath, outPath)

    if create:
        fileEntry = peg.File(lfn)
        if not filePath.startswith(outPath):
            fileEntry.addPFN(peg.PFN(filePath, site="local"))
            fileEntry.addPFN(peg.PFN(filePath, site="lsstvc"))
        logger.debug("%s %s: %s -> %s", datasetType, dataId, filePath, lfn)

    return fileEntry

def generateProcessCcdDax(name="dax", visits=None, ccdList=None):
    """Generate a Pegasus DAX abstract workflow"""
    try:
        from AutoADAG import AutoADAG
    except ImportError:
        dax = peg.ADAG(name)
    else:
        dax = AutoADAG(name)

    # Construct these mappers only for creating dax, not for actual runs.
    mapperInput = HscMapper(root=inputRepo, calibRoot=calibRepo)
    mapper = HscMapper(root=inputRepo, outputRoot=outPath)

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

    configFile = os.path.join(getPackageDir("obs_subaru"), "config", "hsc", "isr.py")
    fringeFilters = []
    with open(configFile, 'r') as f:
        if "fringe.filters" in f.readline():
            fringeFilters = eval(f.split("=")[-1])

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

            inFile = getDataFile(mapperInput, "raw", dataId, create=True, replaceRootPath=inputRepo)
            dax.addFile(inFile)
            processCcd.uses(inFile, link=peg.Link.INPUT)
            for inputType in ["bias", "dark", "flat", "bfKernel"]:
                inFile = getDataFile(mapperInput, inputType, dataId, 
                                     create=True, replaceRootPath=calibRepo)
                if not dax.hasFile(inFile):
                    dax.addFile(inFile)
                processCcd.uses(inFile, link=peg.Link.INPUT)

            filterName = mapperInput.queryMetadata(datasetType="raw", format=("filter",), dataId={'visit':visit})[0][0]
            if filterName in fringeFilters:
                inFile = getDataFile(mapperInput, "fringe", dataId,
                                     create=True, replaceRootPath=calibRepo)
                if not dax.hasFile(inFile):
                    dax.addFile(inFile)
                processCcd.uses(inFile, link=peg.Link.INPUT)

            for outputType in ["calexp", "src"]:
                outFile = getDataFile(mapper, outputType, dataId, create=True)
                dax.addFile(outFile)
                processCcd.uses(outFile, link=peg.Link.OUTPUT)

            logProcessCcd = peg.File("logProcessCcd.v{visit}.c{ccd}".format(**dataId))
            dax.addFile(logProcessCcd)
            processCcd.setStderr(logProcessCcd)
            processCcd.uses(logProcessCcd, link=peg.Link.OUTPUT)

            dax.addJob(processCcd)

    return dax


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a DAX")
    parser.add_argument("-i", "--inputData", default="DM9235/visitsCosmos.txt",
                        help="a file including input data information")
    parser.add_argument("-o", "--outputFile", type=str, default="HscProcessCcd.dax",
                        help="file name for the output dax xml")
    args = parser.parse_args()
    with open(args.inputData) as f:
        visits = [line.rstrip() for line in f]

    ccdList = range(9) + range(10, 104)
    dax = generateProcessCcdDax("HscDax", visits, ccdList)
    with open(args.outputFile, "w") as f:
        dax.writeXML(f)
