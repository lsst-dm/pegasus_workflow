# This is a workaround before DM-10634 is done
# The default assembleCoadd.py (same as in ci_hsc) 
# uses obs_subaru/config/safeClipAssembleCoadd.py 
# WcsSelectImagesTask instead of PsfWcsSelectImagesTask
# while coaddDriver uses config/assembleCoadd.py
config.badMaskPlanes = ("BAD", "EDGE", "SAT", "INTRP", "NO_DATA",)
config.doMatchBackgrounds = False
config.doSigmaClip = False
config.subregionSize = (10000, 200) # 200 rows (since patch width is typically < 10k pixels
config.doMaskBrightObjects = True
from lsst.pipe.tasks.selectImages import PsfWcsSelectImagesTask
config.select.retarget(PsfWcsSelectImagesTask)
