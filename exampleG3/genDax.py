#!/usr/bin/env python
#
# beyond lsst_distrib, setup pipe_supertask


import os
import Pegasus.DAX3 as peg

import lsst.daf.persistence as dafPersist
import lsst.log
import lsst.utils


dax = peg.ADAG("example")

# Add executables
stactask = peg.Executable(name="stac", arch="x86_64", installed=False)
psDir = lsst.utils.getPackageDir('pipe_supertask')
stactask.addPFN(peg.PFN("file://" + psDir + "/bin/stac", "local"))
stactask.addPFN(peg.PFN("file://" + psDir + "/bin/stac", "lsstvc"))
dax.addExecutable(stactask)


repoPath = "/scratch/hchiang2/sw/ci_hsc/DATA"
for visit in range(1, 5):
    outCollection = "out-test%d" % visit
    #inputRaw = peg.File("file.raw.visit%d" % visit)
    #inputRaw.addPFN(peg.PFN(filePath, site="local"))
    #dax.addFile(inputRaw)
    exampleCmdLineTask = peg.Job(name="stac")
    exampleCmdLineTask.addArguments("-b", repoPath, "-i", "raw",
                                    "-o", outCollection, "run", "-t", "RawToCalexpTask")
    #exampleCmdLineTask.uses(inputRaw, link=peg.Link.INPUT)

    result = peg.File("exampleOutput.visit%d" % visit)
    #result = peg.File("exampleOutput")
    exampleCmdLineTask.setStderr(result)
    exampleCmdLineTask.uses(result, link=peg.Link.OUTPUT, transfer=True, register=True)
    dax.addJob(exampleCmdLineTask)


f = open("testdaxfile.dax", "w")
dax.writeXML(f)
f.close()
