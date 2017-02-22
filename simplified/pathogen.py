import os.path


class Pathogen:
    """Path generator.

    In the LSST realm only Bulter/Mapper knows where is the file corresponding
    to a tuple (dataset type, dataset ids).  Pathogen taps to inner workings of
    a mapper to make this information available for the outside world.

    Parameters
    ----------
    mapper : lsst.obs.base.CameraMapper
        A camera mapper to use with the dataset repositories.
    root : `str`
        Root of the input dataset repository.
    output : `str`
        Root of the output dataset repository.
    """
    def __init__(self, mapper, root, output):
        self.root = os.path.abspath(root)
        self.output = output
        self.mapper = mapper(root=self.root)

    def get(self, data_type, data_id):
        """Finds out the path corresponding to a given dataset type and id.

        Parameters
        ----------
        data_type : `str`
            Dataset type.
        data_id : `dict`
            Set of key/value pairs constituting the dataset id.

        Returns
        -------
        tuple of `str`
            Logical and physical filenames corresponding to a given dataset
            type and id.

            If file does not exist, physical name defaults to None.
        """
        mapping = getattr(self.mapper, 'map_' + data_type)
        path = os.path.abspath(mapping(data_id).getLocations()[0])
        pfn = path if os.path.isfile(path) else None
        lfn = path.replace(self.root, self.output)
        return lfn, pfn
