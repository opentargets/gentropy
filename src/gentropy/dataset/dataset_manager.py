"""Dataset dependency manager."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, Literal, TypeVar, Union

from gentropy.common.exceptions import DatasetNotFoundError
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.target_index import TargetIndex
from gentropy.dataset.variant_index import VariantIndex

if TYPE_CHECKING:
    pass


DatasetDerivative = TypeVar(
    "DatasetDerivative",
    bound=Union[
        Colocalisation,
        Dataset,
        StudyIndex,
        StudyLocus,
        TargetIndex,
        VariantIndex,
    ],
)


DatasetName = Literal[
    "colocalisation",
    "study_index",
    "study_locus",
    "target_index",
    "variant_index",
]


class DatasetManager(Generic[DatasetDerivative]):
    """Class to store the information about the datasets."""

    def __init__(self):
        """Construct manager."""
        self.registry: dict[DatasetName, DatasetDerivative] = {}

    def add(self, dataset: DatasetDerivative) -> DatasetManager[DatasetDerivative]:
        """Add dataset to the managers registry in place.

        Args:
            dataset (Dataset): Can be a class derived from the Dataset.

        Returns:
            DatasetManager: Itself.

        Raises:
            TypeError: When added a dataset or type not supported by manager.

        """
        match dataset:
            case StudyLocus():
                self.registry["study_locus"] = dataset
            case StudyIndex():
                self.registry["study_index"] = dataset
            case TargetIndex():
                self.registry["target_index"] = dataset
            case VariantIndex():
                self.registry["variant_index"] = dataset
            case Colocalisation():
                self.registry["colocalisation"] = dataset
            case Dataset():
                raise TypeError(f"Dataset: {type(dataset)} not supported")
            case _:
                raise TypeError(f"Type: {type(dataset)} not allowed.")

        return self

    def get(self, name: DatasetName) -> DatasetDerivative:
        """Retreive the dependency from managers registry.

        Args:
            name (DependencyName): Name of the dependency to retrieve

        Returns:
            Dataset: Dataset derived by the name if the name was added to the registry.

        Raises:
            DependencyError: When the dependency name was not added to the registry before.
        """
        dependency = self.registry.get(name)
        if not dependency:
            raise DatasetNotFoundError(f"Dataset {name} was not found in the registry.")
        return dependency
