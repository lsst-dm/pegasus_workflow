#!/usr/bin/env python
import argparse
import os
import Pegasus.DAX3 as peg

import lsst.log
import lsst.utils
from lsst.obs.hsc.hscMapper import HscMapper

logger = lsst.log.Log.getLogger("workflow")
logger.setLevel(lsst.log.DEBUG)

# hard-coded output repo
# A local output repo is written when running this script;
# this local repo is not used at all for actual job submission and run.
# Real submitted run dumps output in scratch (specified in the site catalog).
outPath = 'peg'
logger.debug("outPath: %s", outPath)

# The input data repo contains all data needed by pipe_analysis pipelines
rootRepo = "/datasets/hsc/repo/"
inputRepo = "/datasets/hsc/repo/rerun/RC/w_2018_26/DM-14689/"
calibRepo = os.path.join(rootRepo, "CALIB")
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
        or None if such file does not exist
    """
    mapFunc = getattr(mapper, "map_" + datasetType)
    butlerPath = mapFunc(dataId).getLocations()[0]
    fileEntry = lfn = os.path.join(outPath, butlerPath)

    if create:
        fileEntry = peg.File(lfn)
        if repoRoot is not None:
            filePath = os.path.join(repoRoot, butlerPath)
            if not os.path.isfile(filePath):
                logger.info("Skip %s %s; there is no %s ", datasetType, dataId, filePath)
                return None
            fileEntry.addPFN(peg.PFN(filePath, site="local"))
            fileEntry.addPFN(peg.PFN(filePath, site="lsstvc"))
            logger.debug("%s %s: %s -> %s", datasetType, dataId, filePath, lfn)

    return fileEntry


def generateDax(name="dax"):
    """Generate a Pegasus DAX abstract workflow"""
    try:
        from AutoADAG import AutoADAG
    except ImportError:
        dax = peg.ADAG(name)
    else:
        dax = AutoADAG(name)

    # Construct these mappers only for creating dax, not for actual runs.
    mapper = HscMapper(root=rootRepo, calibRoot=calibRepo)

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

    # Pipeline: visitAnalysis for each visit
    for data in allData:
        logger.debug("visitAnalysis dataId: %s", data)

        visitAnalysis = peg.Job(name="visitAnalysis")
        visitAnalysis.addArguments(outPath, "--calib", outPath,
                                   "--output", outPath, " --doraise",
                                   "--tract=%s" % data['tract'],
                                   "--id visit=%s" % data['visit'])
        visitAnalysis.uses(registry, link=peg.Link.INPUT)
        visitAnalysis.uses(mapperFile, link=peg.Link.INPUT)
        for inputType in ["src_schema"]:
            inFile = getDataFile(mapper, inputType, {}, create=True, repoRoot=inputRepo)
            if not dax.hasFile(inFile):
                dax.addFile(inFile)
            visitAnalysis.uses(inFile, link=peg.Link.INPUT)

        # All its input files are stored per ccd
        for ccdId in range(0, 104):
            if ccdId == 9:
                continue
            for inputType in ["calexp", "src", "srcMatch"]:
                inFile = getDataFile(mapper, inputType, 
                                     dict(visit=data['visit'], ccd=ccdId),
                                     create=True, repoRoot=inputRepo)
                if not dax.hasFile(inFile):
                    dax.addFile(inFile)
                visitAnalysis.uses(inFile, link=peg.Link.INPUT)

            # jointcal_wcs does not exist for all CCDs
            inFile = getDataFile(mapper, "jointcal_wcs",
                                 dict(tract=data['tract'], visit=data['visit'], ccd=ccdId),
                                 create=True, repoRoot=inputRepo)
            if inFile is not None:
                dax.addFile(inFile)
                visitAnalysis.uses(inFile, link=peg.Link.INPUT)

        # Need skymap via PerTractCcdDataIdContainer
        skyMap = getDataFile(mapper, "deepCoadd_skyMap", {},
                             create=True, repoRoot=inputRepo)
        if not dax.hasFile(skyMap):
            dax.addFile(skyMap)
        visitAnalysis.uses(skyMap, link=peg.Link.INPUT)

        # Output "plot-vN-[matches_]schemaItem[_subset]_plotType.png"
        # yet to be added

        logVisitAnalysis = peg.File("logVisitAnalysis-t{tract}-v{visit}".format(**data))
        dax.addFile(logVisitAnalysis)
        visitAnalysis.setStdout(logVisitAnalysis)
        visitAnalysis.uses(logVisitAnalysis, link=peg.Link.OUTPUT)

        dax.addJob(visitAnalysis)


    return dax


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a DAX")
    parser.add_argument("-i", "--inputData", default="pipeAnalysis/inputDataRC2.py",
                        help="a file including input data information")
    parser.add_argument("-o", "--outputFile", type=str, default="pa.dax",
                        help="file name for the output dax xml")
    args = parser.parse_args()
    with open(args.inputData) as f:
        data = compile(f.read(), args.inputData, 'exec')
        exec(data)

    dax = generateDax("PaDax")
    with open(args.outputFile, "w") as f:
        dax.writeXML(f)
