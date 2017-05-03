#!/usr/bin/env python

import lsst.afw.geom as afwGeom
import lsst.daf.persistence as dafPersist
import lsst.log
from lsst.meas.astrom.ref_match import RefMatchTask
from lsst.meas.algorithms import LoadIndexedReferenceObjectsTask, LoadIndexedReferenceObjectsConfig

logger = lsst.log.Log.getLogger("findShardId")
logger.setLevel(lsst.log.DEBUG)

def findShardId(butler, expId, expType="raw", ref_dataset_name="ps1_pv3_3pi_20170110"):
    """Obtain the shard IDs given (an exposure and the loader)

    Parameters
    ----------
    butler: A daf.persistence.Butler object;
        to be used to instantiate LoadIndexedReferenceObjectsTask
    loader: meas.algorithms.LoadIndexedReferenceObjectsTask
        A loader task instantiated with a butler
    exp: an exposure
    expId: a dataId dict for the exposure
    expType: a butler type to retrieve the exposure
    ref_dataset_name: a HTM cat name accessible from the given butler repo

    Returns
    -------
    shardId: a butler dataId for the shard pixel_id,
        to retrieve the shards of Butler dataset type "ref_cat"
    """
    config = LoadIndexedReferenceObjectsConfig()
    config.ref_dataset_name = ref_dataset_name
    loader = LoadIndexedReferenceObjectsTask(butler=butler, config=config)

    refMatchTask = RefMatchTask(refObjLoader=loader)
    exp = butler.get(expType, expId)
    expMd = refMatchTask._getExposureMetadata(exp)

    # Copied from LoadReferenceObjectsTask.loadPixelBox
    # compute on-sky center and radius of search region
    bbox = afwGeom.Box2D(expMd.bbox) # make sure bbox is double and that we have a copy
    bbox.grow(config.pixelMargin)
    ctrCoord = expMd.wcs.pixelToSky(bbox.getCenter())
    maxRadius = max(ctrCoord.angularSeparation(expMd.wcs.pixelToSky(pp)) for pp in bbox.getCorners())
    # Copied from LoadIndexedReferenceObjectsTask.loadSkyCircle and get_shards
    id_list, boundary_mask = loader.indexer.get_pixel_ids(ctrCoord, maxRadius)
    # loader.get_shards(id_list)
    shardPixels = []
    for pixel_id in id_list:
        shard_id = loader.indexer.make_data_id(pixel_id, config.ref_dataset_name)
        #butler.get('ref_cat', dataId=shard_id)
        shardPixels.append(shard_id['pixel_id'])

    logger.debug("shard_id pixel_id is %s" % shardPixels)
    return shardPixels
