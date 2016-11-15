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
logger.debug("outPath: %s",outPath)

# Assuming ci_hsc has been run beforehand and the data repo has been created
ciHscDir = lsst.utils.getPackageDir('ci_hsc')
inputRepo = os.path.join(ciHscDir, "DATA")

# Construct these butler and mappers only for creating dax, not for actual runs.
inputArgs = dafPersist.RepositoryArgs(mode='r', mapper=HscMapper, root=inputRepo) # read-only input
outputArgs = dafPersist.RepositoryArgs(mode='w', mapper=HscMapper, root=outPath) # write-only output
butler = dafPersist.Butler(inputs=inputArgs, outputs=outPath)
mapperInput = HscMapper(root=inputRepo)
mapper = HscMapper(root=inputRepo, outputRoot=outPath)

dax = peg.ADAG("CiHscDax")

filePathMapper = os.path.join(inputRepo, "_mapper")
mapperFile = peg.File(os.path.join(outPath, "_mapper"))
mapperFile.addPFN(peg.PFN(filePathMapper, site="local"))
dax.addFile(mapperFile)

filePathRegistry = os.path.join(inputRepo, "registry.sqlite3")
registry = peg.File(os.path.join(outPath, "registry.sqlite3"))
registry.addPFN(peg.PFN(filePathRegistry, site="local"))
dax.addFile(registry)

calexpList = []
tasksProcessCcdList = []

for data in sum(allData.itervalues(), []):
    logger.debug("processCcd dataId: %s", data.dataId)

    filePathRaw = mapperInput.map_raw(data.dataId).getLocations()[0]
    inputRaw = peg.File(os.path.basename(filePathRaw))
    inputRaw.addPFN(peg.PFN(filePathRaw, site="local"))
    logger.debug("dataId: %s input filePathRaw: %s", data.name, filePathRaw)
    dax.addFile(inputRaw)

    processCcd = peg.Job(name="processCcd")
    processCcd.addArguments(inputRepo, "--output", outPath, "--no-versions",
                            data.id())
    processCcd.uses(inputRaw, link=peg.Link.INPUT)
    processCcd.uses(registry, link=peg.Link.INPUT)
    processCcd.uses(mapperFile, link=peg.Link.INPUT)

    filePathCalexp = mapper.map_calexp(data.dataId).getLocations()[0]
    calexp = peg.File(filePathCalexp)
    calexp.addPFN(peg.PFN(filePathCalexp, site="local"))
    logger.debug("dataId %s output filePathCalexp: %s", data.name, filePathCalexp)

    filePathSrc = mapper.map_src(data.dataId).getLocations()[0]
    src = peg.File(filePathSrc)
    src.addPFN(peg.PFN(filePathSrc, site="local"))
    logger.debug("dataId %s output filePathSrc: %s", data.name, filePathSrc)

    processCcd.uses(calexp, link=peg.Link.OUTPUT, transfer=True, register=True)
    processCcd.uses(src, link=peg.Link.OUTPUT, transfer=True, register=True)

    logProcessCcd = peg.File("logProcessCcd.%s" % data.name)
    processCcd.setStderr(logProcessCcd)
    processCcd.uses(logProcessCcd, link=peg.Link.OUTPUT)

    dax.addJob(processCcd)

    calexpList.append(calexp)
    tasksProcessCcdList.append(processCcd)

makeSkyMap = peg.Job(name="makeDiscreteSkyMap")
makeSkyMap.uses(mapperFile, link=peg.Link.INPUT)
makeSkyMap.uses(registry, link=peg.Link.INPUT)
makeSkyMap.addArguments(outPath, "--output", outPath, "--no-versions",
                        " ".join(data.id() for data in sum(allData.itervalues(), [])))
logger.debug("Adding makeSkyMap with dataId: %s",
             " ".join(data.id() for data in sum(allData.itervalues(), [])))
logMakeSkyMap = peg.File("logMakeSkyMap")
makeSkyMap.setStderr(logMakeSkyMap)
makeSkyMap.uses(logMakeSkyMap, link=peg.Link.OUTPUT)

for calexp in calexpList:
    makeSkyMap.uses(calexp, link=peg.Link.INPUT)

filePathSkyMap = mapper.map_deepCoadd_skyMap({}).getLocations()[0]
skyMap = peg.File(filePathSkyMap)
skyMap.addPFN(peg.PFN(filePathSkyMap, site="local"))
logger.debug("filePathSkyMap: %s", filePathSkyMap)
makeSkyMap.uses(skyMap, link=peg.Link.OUTPUT, transfer=True, register=True)

dax.addJob(makeSkyMap)
for job in tasksProcessCcdList:
    dax.depends(makeSkyMap, job)

f = open("ciHsc.dax", "w")
dax.writeXML(f)
f.close()
