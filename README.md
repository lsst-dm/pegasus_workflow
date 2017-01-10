Setup
-----

- Setup the LSST Stack lsst_apps and ci_hsc

- Run ci_hsc (execute scons) to ingest images and make a Butler data repo if it has not been done. `generateDax.py` assumes the Butler repo `ci_hsc/DATA/` already exists and is ready for use.

- Obtain a copy of AutoADAG.py from e.g. [here](https://github.com/pegasus-isi/pegasus-gtfar/blob/bd092b7adbd3e2fb70679cbb58681ec26b74602a/pegasus/gtfar/dax/AutoADAG.py)

- For running jobs on the worker nodes of the LSST Verification Cluster,
  first allocate nodes using tools in ctrl_execute and obtain a node set name.
  Set env var `NODESET` to that name, e.g.:
  ```
  export NODESET=${USER}_`cat ~/.lsst/node-set.seq`
  ```


Steps (with the ciHsc example)
------------------------------

- python ciHsc/generateDax.py -i ciHsc/inputData.py
- ./plan_dax.sh ciHsc.dax


Examples of using Pegasus Tools
-------------------------------

- pegasus-plots -o plotsDir -p dax_graph -f submit/centos/pegasus/CiHscDax/run0001/
- pegasus-plots -o plotsDir -p dag_graph submit/centos/pegasus/CiHscDax/run0001/
- pegasus-analyzer submit/centos/pegasus/CiHscDax/run0001/
- Documentation: https://pegasus.isi.edu/documentation/cli.php
