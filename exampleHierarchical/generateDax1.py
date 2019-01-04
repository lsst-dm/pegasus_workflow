#!/usr/bin/env python

import Pegasus.DAX3 as peg
import os
import re
import sys

run_dir = sys.argv[1]
input_full_path = sys.argv[2]
base_dir = os.getcwd()

dax = peg.ADAG("fake")

# Add executables to the DAX-level replica catalog
taskA = peg.Executable(name="taskA.sh", arch="x86_64", installed=False)
taskA.addPFN(peg.PFN("file://" + base_dir + "/taskA.sh", "local"))
dax.addExecutable(taskA)

gendax2 = peg.Executable(name="generateDax2.py", arch="x86_64", installed=False)
gendax2.addPFN(peg.PFN("file://" + base_dir + "/generateDax2.py", "local"))
dax.addExecutable(gendax2)


# create Pegasus file objects
input_filename = re.sub(".*/", "", input_full_path)
input_file = peg.File(input_filename)
input_file.addPFN(peg.PFN("file://" + input_full_path, "local"))
dax.addFile(input_file)

job1 = peg.Job(name="taskA.sh")
job1.addProfile(peg.Profile("hints", "execution.site", "local"))
job1.uses(input_file, link=peg.Link.INPUT)
job1.addArguments(run_dir, input_file)
dax.addJob(job1)

job2 = peg.Job(name="generateDax2.py")
job2.addProfile(peg.Profile("hints", "execution.site", "local"))
job2.addArguments(base_dir, run_dir)
dax.addJob(job2)
dax.depends(parent=job1, child=job2)

subdax_file = peg.File("dax2.xml")
subdax_file.addPFN(peg.PFN("file://%s/dax2.xml" % (run_dir), "local"))
dax.addFile(subdax_file)

job3 = peg.DAX("dax2.xml")
job3.addArguments("-Dpegasus.catalog.site.file=%s/sites.xml" % (base_dir),
                  "--sites", "condorpool",
                  "--output-site", "local",
                  "--basename", "dax2",
                 )
job3.uses(subdax_file, link=peg.Link.INPUT)
dax.addDAX(job3)
dax.depends(parent=job2, child=job3)

# Write the DAX to stdout
f = open("dax1.xml", "w")
dax.writeXML(f)
f.close()
