#!/usr/bin/env python
#
# beyond lsst_distrib, setup ctrl_mpexec


import os
import sqlite3
import Pegasus.DAX3 as peg

import lsst.daf.persistence as dafPersist
import lsst.log
import lsst.utils

try:
    from AutoADAG import AutoADAG
except ImportError:
    dax = peg.ADAG(name)
else:
    dax = AutoADAG(name)

# Add executables
pipetask = peg.Executable(name="pipetask", arch="x86_64", installed=False)
cmDir = lsst.utils.getPackageDir('ctrl_mpexec')
pipetask.addPFN(peg.PFN("file://" + cmDir + "/bin/pipetask", "local"))
pipetask.addPFN(peg.PFN("file://" + cmDir + "/bin/pipetask", "lsstvc"))
dax.addExecutable(pipetask)


repoPath = "/scratch/hchiang2/sw/ci_hsc/DATA"
conn = sqlite3.connect(os.path.join(repoPath, "gen3.sqlite3"))
c = conn.cursor()
inputType = "raw"
query = 'select visit,GROUP_CONCAT(PosixDatastoreRecords.path) from Dataset inner join PosixDatastoreRecords on Dataset.dataset_id = PosixDatastoreRecords.dataset_id where  dataset_type_name ="%s" group by visit;' % inputType;

outCollection = "outs/out2"
for _ in c.execute(query):
    visit = _[0]
    paths = _[1]
    exampleCmdLineTask = peg.Job(name="pipetask")
    exampleCmdLineTask.addArguments("-b", repoPath, "-i", inputType, "-o", outCollection,
                                    "-d", "Visit.visit=%d" % visit, "run",
                                    "-t", "rawToCalexpTask.RawToCalexpTask")
    for path in paths.split(','):
        fullPath = os.path.join(repoPath, path)
        inputRaw = peg.File(os.path.basename(path))
        inputRaw.addPFN(peg.PFN(fullPath, site="local"))
        dax.addFile(inputRaw)
        exampleCmdLineTask.uses(inputRaw, link=peg.Link.INPUT)

    result = peg.File("exampleOutput.visit%d" % visit)
    #result = peg.File("exampleOutput")
    exampleCmdLineTask.setStderr(result)
    exampleCmdLineTask.uses(result, link=peg.Link.OUTPUT, transfer=True, register=True)
    dax.addJob(exampleCmdLineTask)

conn.close()
f = open("testdaxfile.dax", "w")
dax.writeXML(f)
f.close()
