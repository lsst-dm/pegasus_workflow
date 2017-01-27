# Information of the raw data in ci_hsc
# Excerpted from ci_hsc/SConstruct
from data import Data


allData = {"HSC-R": [Data(903334, 16),
                     Data(903334, 23),
                     Data(903336, 17),
                     Data(903336, 24),
                     ],
           }

patchDataId = dict(tract=0, patch="8,6")
