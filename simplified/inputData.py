# Information of the raw data in ci_hsc
# Excerpted from ci_hsc/SConstruct
from collections import defaultdict
from data import Data


allData = {"HSC-R": [Data(903334, 16),
                     Data(903334, 23),
                     Data(903336, 17),
                     Data(903336, 24),
                     ],
           }

patchDataId = dict(tract=0, patch="8,6")
patchId = " ".join(("%s=%s" % (k, v) for k, v in patchDataId.iteritems()))

# Create "exposures" as in ci_hsc/SConstruct processCoadds
allExposures = {filterName: defaultdict(list) for filterName in allData}
for filterName in allData:
    for data in allData[filterName]:
        allExposures[filterName][data.visit].append(data)
