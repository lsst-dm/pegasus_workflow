Setup
-----

- Setup the LSST Stack lsst_apps and ci_hsc
- Modify sites.xml and tc.txt as necessary
- Obtain a copy of AutoADAG.py from e.g. [here](https://github.com/pegasus-isi/pegasus-gtfar/blob/bd092b7adbd3e2fb70679cbb58681ec26b74602a/pegasus/gtfar/dax/AutoADAG.py)


Steps
-----

- python generateDax.py
- ./plan_dax.sh ciHsc.dax


Examples of using Pegasus Tools
-------------------------------

- pegasus-plots -o plotsDir -p dax_graph -f submit/centos/pegasus/CiHscDax/run0001/
- pegasus-plots -o plotsDir -p dag_graph submit/centos/pegasus/CiHscDax/run0001/
- pegasus-analyzer submit/centos/pegasus/CiHscDax/run0001/
- Documentation: https://pegasus.isi.edu/documentation/cli.php
