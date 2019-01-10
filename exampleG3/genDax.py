#!/usr/bin/env python
#
# beyond lsst_distrib, setup ctrl_mpexec


import os
import Pegasus.DAX3 as peg

import lsst.daf.persistence as dafPersist
import lsst.log
import lsst.utils


dax = peg.ADAG("example")

# Add executables
pipetask = peg.Executable(name="pipetask", arch="x86_64", installed=False)
cmDir = lsst.utils.getPackageDir('ctrl_mpexec')
pipetask.addPFN(peg.PFN("file://" + cmDir + "/bin/pipetask", "local"))
pipetask.addPFN(peg.PFN("file://" + cmDir + "/bin/pipetask", "lsstvc"))
dax.addExecutable(pipetask)


repoPath = "/scratch/hchiang2/sw/ci_hsc/DATA"
for visit in range(1, 5):
    outCollection = "out-test%d" % visit
    #inputRaw = peg.File("file.raw.visit%d" % visit)
    #inputRaw.addPFN(peg.PFN(filePath, site="local"))
    #dax.addFile(inputRaw)
    exampleCmdLineTask = peg.Job(name="pipetask")
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
