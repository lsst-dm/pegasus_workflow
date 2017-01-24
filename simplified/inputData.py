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

allData = {"HSC-R": [Data(903334, 16),
                     Data(903334, 23),
                     Data(903336, 17),
                     Data(903336, 24),
                     Data(903342, 4),
                     Data(903342, 10),
                     Data(903344, 5),
                     Data(903344, 11),
                     ],
           }

patchDataId = dict(tract=0, patch="5,4")
patchId = " ".join(("%s=%s" % (k, v) for k, v in patchDataId.iteritems()))

# Create "exposures" as in ci_hsc/SConstruct processCoadds
allExposures = {filterName: defaultdict(list) for filterName in allData}
for filterName in allData:
    for data in allData[filterName]:
        allExposures[filterName][data.visit].append(data)
