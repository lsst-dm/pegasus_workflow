#!/usr/bin/env python
from collections import defaultdict
import os
import sqlite3
from lsst.pipe.base import Struct


conn = sqlite3.connect('mock.sqlite3')
cur = conn.cursor()
# Assume there is only one tract with tract=0
cur.execute("select distinct tract from skymock;")
tractDataId = cur.fetchone()[0]
assert tractDataId == 0

# Needed by processCcd and forcedPhotCcd
cur.execute("select distinct visit,ccd from skymock;")
allCcds = cur.fetchall()
cur.close()

# Obtain allFilters and allPatches
conn.row_factory = lambda cursor, row: row[0]
cur = conn.cursor()
cur.execute("select distinct filter from raw;")
allFilters = cur.fetchall()

cur.execute("select distinct patch from skymock;")
allPatches = cur.fetchall()
cur.close()

# references mapping for forcedPhotCcd
cur = conn.cursor()
references = defaultdict(list)
for data in allCcds:
    cmd = "select patch from skymock where visit=%s and ccd=%s;" % (data[0], data[1])
    cur.execute(cmd)
    overlappedPatches = cur.fetchall()
    for patch in overlappedPatches:
        references[data].append(patch)

cur.close()

# Needed by makeCoaddTempExp
conn.row_factory = None
cur = conn.cursor()
allExposures = {filterName: {patch: defaultdict(list) for patch in allPatches} for filterName in allFilters}
for filterName in allFilters:
    cmd = "select distinct raw.visit from skymock join raw on skymock.visit == raw.visit where raw.filter='%s';" % filterName
    cur.execute(cmd)
    visits = cur.fetchall()
    for visit in visits:
        cmd = "select patch,ccd from skymock where visit=%d;" % visit[0]
        cur.execute(cmd)
        rows = cur.fetchall()
        for row in rows:
            allExposures[filterName][row[0]][visit[0]].append(row[1])

cur.close()

# Needed by assembleCoadd and measureCoaddSources
skyMapping = {filterName: defaultdict(list) for filterName in allFilters}
for filterName in allFilters:
    for patch in allPatches:
        for visit in allExposures[filterName][patch]:
            for ccd in allExposures[filterName][patch][visit]:
                skyMapping[filterName][patch].append((visit, ccd))
        print filterName, patch, skyMapping[filterName][patch]

print allCcds
print allFilters, allPatches
