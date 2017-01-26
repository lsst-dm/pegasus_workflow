#!/usr/bin/env python
import os
import sqlite3
import lsst.utils


ciHscDir = lsst.utils.getPackageDir('ci_hsc')
inputRepo = os.path.join(ciHscDir, "DATA")
ciHscRegistry = os.path.join(inputRepo, "registry.sqlite3")

conn = sqlite3.connect('mock.sqlite3')
cur = conn.cursor()
cur.execute("drop table if exists raw")
cur.execute("attach database ? as dbInput", (ciHscRegistry,))
# Get the table schema from the input db
cur.execute("select sql from dbInput.sqlite_master where type='table' and name='raw';")
cmd = cur.fetchone()[0]
print("Copy the raw table from ci_hsc registry %s" % cmd)
# Create a raw table of the same schema
cur.execute(cmd)
# Copy the table data over 
cur.execute("insert into main.raw select * from dbInput.raw")

conn.commit()

# Cheat and falsely assume we know the input data Ids of interest
# and we know what exposures overlap what patches.
# This is only known after some processing steps but can be computed.
skyMapping = {
    "8,7": [(903334, 23), (903336, 24), (903342, 4),
            (903342, 10), (903344, 5), (903344, 11),
            (903986, 23), (904010, 4), (904010, 10),
            (903990, 25), (904014, 6), (904014, 12)],
    "9,7": [(903334, 23), (903336, 24), (903342, 4),
            (903342, 10), (903344, 5), (903344, 11),
            (904010, 4), (904010, 10), (903986, 23),
            (904014, 6), (904014, 12), (903990, 25)],
    "8,6": [(903334, 16), (903334, 23), (903336, 24),
            (903336, 17), (903342, 4), (903344, 5),
            (904010, 4), (903986, 16), (903986, 23),
            (904014, 6), (903990, 18), (903990, 25)],
    "9,6": [(903334, 16), (903334, 23), (903336, 24),
            (903336, 17), (903342, 4), (903344, 5),
            (904010, 4), (903986, 16), (903986, 23),
            (904014, 6), (903990, 18), (903990, 25)]
}

# Create a table for patch lookup
table = "skymock"
columns = {'visit': 'int', 'ccd': 'int', 'tract': 'int', 'patch': 'text'}
cmd = "create table %s (id integer primary key autoincrement, " % table
cmd += ",".join([("%s %s" % (col, colType)) for col, colType in columns.items()])
cmd += ")"
cur.execute("drop table if exists %s" % table)
conn.execute(cmd)
conn.commit()

for patch in skyMapping:
    cmd = "insert into %s (visit, ccd, tract, patch) values (?, ?, 0, '%s')" % (table, patch)
    conn.executemany(cmd, skyMapping[patch])

conn.commit()
conn.close()
