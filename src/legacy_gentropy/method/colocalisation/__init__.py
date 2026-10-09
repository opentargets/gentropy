"""Colocalisation methods."""

from enum import Enum

from gentropy.method.colocalisation.coloc import Coloc
from gentropy.method.colocalisation.coloc_pip import ColocPIP
from gentropy.method.colocalisation.coloc_pip_ecaviar import ColocPIPECaviar
from gentropy.method.colocalisation.ecaviar import ECaviar
from gentropy.method.colocalisation.model import ColocalisationMethodInterface


class InvalidColocalisationMethodError(Exception):
    """Raise when invalid colocalisation method was chosen."""


class ColocalisationMethod(Enum):
    """Colocalisation methods enum.

    This is the main entry point to get the colocalisation method class.
    """

    COLOC = Coloc
    """Colocalisation method that uses the coloc approach."""
    ECAVIAR = ECaviar
    """Colocalisation method that uses the eCaviar approach."""
    COLOC_PIP_ECAVIAR = ColocPIPECaviar
    """Colocalisation method that runs both ColocPIP and eCAVIAR."""
    COLOC_PIP = ColocPIP
    """Colocalisation method that uses the ColocPIP approach."""

    @classmethod
    def get_method_class(cls, label: str) -> type[ColocalisationMethodInterface]:
        """Get colocalisation method class.

        Args:
            label (str): colocalisation method name

        Returns:
            type[ColocalisationMethodInterface]: colocalisation method class

        Raises:
            InvalidColocalisationMethodError: If colocalisation method not found.

        Examples:
            >>> ColocalisationMethod.get_method_class('coloc')
            <class 'gentropy.method.colocalisation.coloc.Coloc'>
            >>> ColocalisationMethod.get_method_class('ecaviar')
            <class 'gentropy.method.colocalisation.ecaviar.ECaviar'>
            >>> ColocalisationMethod.get_method_class('coloc_pip')
            <class 'gentropy.method.colocalisation.coloc_pip.ColocPIP'>
            >>> ColocalisationMethod.get_method_class('coloc_pip_ecaviar')
            <class 'gentropy.method.colocalisation.coloc_pip_ecaviar.ColocPIPECaviar'>
        """
        label = label.upper()
        match label:
            case cls.COLOC.name:
                return cls.COLOC.value
            case cls.ECAVIAR.name:
                return cls.ECAVIAR.value
            case cls.COLOC_PIP.name:
                return cls.COLOC_PIP.value
            case cls.COLOC_PIP_ECAVIAR.name:
                return cls.COLOC_PIP_ECAVIAR.value
            case _:
                raise InvalidColocalisationMethodError(
                    f"Colocalisation method {label} not available."
                )

    @classmethod
    def get_method_names_for_metric(cls, metric: str) -> list[str]:
        """Get colocalisation method names that produce a given metric.

        Args:
            metric (str): colocalisation method metric (e.g. h4, clpp)

        Returns:
            list[str]: list of colocalisation method names that produce the given metric

        Raises:
            ValueError: If colocalisation method metric not found.

        Examples:
            >>> ColocalisationMethod.get_method_names_for_metric('h4')
            ['COLOC', 'COLOC_PIP_ECAVIAR', 'COLOC_PIP']
            >>> ColocalisationMethod.get_method_names_for_metric('clpp')
            ['eCAVIAR', 'COLOC_PIP_ECAVIAR']
        """
        metric = metric.lower()
        match metric:
            case "h4":
                return [
                    c.value.METHOD_NAME for c in cls if "h4" in c.value.METHOD_METRICS
                ]
            case "clpp":
                return [
                    c.value.METHOD_NAME for c in cls if "clpp" in c.value.METHOD_METRICS
                ]
            case _:
                raise ValueError(
                    f"Colocalisation method metric {metric} not available."
                )


__all__ = ["ColocalisationMethod"]
