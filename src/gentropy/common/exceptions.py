"""Custom exceptions."""


class GentropyException(BaseException):
    """Base class for all gentropy-specific exceptions."""


class L2GFeatureError(GentropyException):
    """Raise when requested L2GFeature can not be found in defined feature namespace."""


class DatasetNotFoundError(GentropyException):
    """Raise when the dependency dataset can not be requested."""
