#!/usr/bin/env python

import os
import Pegasus.DAX3 as peg
import lsst.log

logger = lsst.log.Log.getLogger("getDataFile")
logger.setLevel(lsst.log.WARN)

def getDataFile(mapper, datasetType, dataId, outPath="repo", create=False, repoRoot=None):
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
    outPath: `str`
        A folder name used as a hack to use the CmdLineTask framework
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
