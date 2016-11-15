Setup
-----

- Setup the LSST Stack lsst_apps and ci_hsc
- Modify sites.xml and tc.txt as necessary


Steps
-----

- python generateDax.py
- ./plan_dax.sh ciHsc.dax


To plot DAX and DAG
-------------------

- pegasus-plots -o plotsDir -p dax_graph submit/centos/pegasus/CiHscDax/run0001/
- pegasus-plots -o plotsDir -p dag_graph submit/centos/pegasus/CiHscDax/run0001/
