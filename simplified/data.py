from lsst.pipe.base import Struct


class Data(Struct):
    """Represents a piece of data to be processed.

    Parameters
    ----------
    visit : `int`
        Visit number.
    ccd : `int`
        Camera CCD number.
    """

    def __init__(self, visit, ccd):
        Struct.__init__(self, visit=visit, ccd=ccd)

    @property
    def name(self):
        """Returns a suitable name for this data.
        """
        return "%d-%d" % (self.visit, self.ccd)

    @property
    def dataId(self):
        """Returns the dataId.
        """
        return dict(visit=self.visit, ccd=self.ccd)

    def id(self, prefix="--id", tract=None):
        """Creates a suitable command-line string.

        Parameters
        ----------
        prefix : `str`, optional
            Prefix required to create valid command line string. Defaults to
            '--id'.
        tract : `int`, optional
            Tract number, defaults to None.

        Returns
        -------
        `str`
            A string representing a valid command line arguments.
        """
        r = "%s visit=%d ccd=%d" % (prefix, self.visit, self.ccd)
        if tract is not None:
            r += " tract=%d" % tract
        return r
