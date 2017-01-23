# Information of the raw data in ci_hsc
# Excerpted from ci_hsc/SConstruct
from collections import defaultdict
from lsst.pipe.base import Struct


class Data(Struct):
    """Data we can process"""

    def __init__(self, visit, ccd):
        Struct.__init__(self, visit=visit, ccd=ccd)

    @property
    def name(self):
        """Returns a suitable name for this data"""
        return "%d-%d" % (self.visit, self.ccd)

    @property
    def dataId(self):
        """Returns the dataId for this data"""
        return dict(visit=self.visit, ccd=self.ccd)

    def id(self, prefix="--id", tract=None):
        """Returns a suitable --id command-line string"""
        r = "%s visit=%d ccd=%d" % (prefix, self.visit, self.ccd)
        if tract is not None:
            r += " tract=%d" % tract
        return r

# Needed by processCcd and forcedPhotCcd
allCcds = {"HSC-R": [Data(903334, 16),
                     Data(903334, 23),
                     Data(903336, 17),
                     Data(903336, 24),
                     Data(903342, 4),
                     Data(903342, 10),
                     Data(903344, 5),
                     Data(903344, 11),
                     ],
           "HSC-I": [Data(903986, 16),
                     Data(903986, 23),
                     Data(904014, 6),
                     Data(904014, 12),
                     Data(903990, 18),
                     Data(903990, 25),
                     Data(904010, 4),
                     Data(904010, 10),
                     ],
           }

tractDataId = 0
allPatches = ['8,7', '9,7', '8,6', '9,6']

# Needed by assembleCoadd and measureCoaddSources
skyMapping = {
    "HSC-R": {
        "8,7": [Data(903334, 23), Data(903336, 24), Data(903342, 4),
                Data(903342, 10), Data(903344, 5), Data(903344, 11)],
        "9,7": [Data(903334, 23), Data(903336, 24), Data(903342, 4),
                Data(903342, 10), Data(903344, 5), Data(903344, 11)],
        "8,6": [Data(903334, 16), Data(903334, 23), Data(903336, 24),
                Data(903336, 17), Data(903342, 4), Data(903344, 5)],
        "9,6": [Data(903334, 16), Data(903334, 23), Data(903336, 24),
                Data(903336, 17), Data(903342, 4), Data(903344, 5)]
    },
    "HSC-I": {
        "8,7": [Data(903986, 23), Data(904010, 4), Data(904010, 10),
                Data(903990, 25), Data(904014, 6), Data(904014, 12)],
        "9,7": [Data(904010, 4), Data(904010, 10), Data(903986, 23),
                Data(904014, 6), Data(904014, 12), Data(903990, 25)],
        "8,6": [Data(904010, 4), Data(903986, 16), Data(903986, 23),
                Data(904014, 6), Data(903990, 18), Data(903990, 25)],
        "9,6": [Data(904010, 4), Data(903986, 16), Data(903986, 23),
                Data(904014, 6), Data(903990, 18), Data(903990, 25)]
    }
}

# references mapping for forcedPhotCcd
references = defaultdict(list)
for data in sum(allCcds.itervalues(), []):
    for filterName in skyMapping:
        for patch in skyMapping[filterName]:
            if data in skyMapping[filterName][patch]:
                references[data].append(patch)

# Needed by makeCoaddTempExp
allExposures = {filterName: {patch: defaultdict(list) for patch in skyMapping[filterName]} for filterName in skyMapping}
for filterName in skyMapping:
    for patchDataId in skyMapping[filterName]:
        for data in skyMapping[filterName][patchDataId]:
            allExposures[filterName][patchDataId][data.visit].append(data)
