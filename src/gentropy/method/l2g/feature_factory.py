# isort: skip_file
"""Factory that computes features based on an input list."""

from __future__ import annotations
from gentropy.dataset.l2g_features.namespace import L2GFeatureName
from gentropy.common.exceptions import L2GFeatureError


from gentropy.dataset.l2g_features.colocalisation import (
    EQtlColocClppMaximumFeature,
    EQtlColocClppMaximumNeighbourhoodFeature,
    EQtlColocH4MaximumFeature,
    EQtlColocH4MaximumNeighbourhoodFeature,
    PQtlColocClppMaximumFeature,
    PQtlColocClppMaximumNeighbourhoodFeature,
    PQtlColocH4MaximumFeature,
    PQtlColocH4MaximumNeighbourhoodFeature,
    SQtlColocClppMaximumFeature,
    SQtlColocClppMaximumNeighbourhoodFeature,
    SQtlColocH4MaximumFeature,
    SQtlColocH4MaximumNeighbourhoodFeature,
)
from gentropy.dataset.l2g_features.distance import (
    DistanceFootprintMeanFeature,
    DistanceFootprintMeanNeighbourhoodFeature,
    DistanceSentinelFootprintFeature,
    DistanceSentinelFootprintNeighbourhoodFeature,
    DistanceSentinelTssFeature,
    DistanceSentinelTssNeighbourhoodFeature,
    DistanceTssMeanFeature,
    DistanceTssMeanNeighbourhoodFeature,
)
from gentropy.dataset.l2g_features.other import (
    CredibleSetConfidenceFeature,
    GeneCountFeature,
    ProteinGeneCountFeature,
    ProteinCodingFeature,
)
from gentropy.dataset.l2g_features.vep import (
    VepMaximumFeature,
    VepMaximumNeighbourhoodFeature,
    VepMeanFeature,
    VepMeanNeighbourhoodFeature,
)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping
    from gentropy.dataset.dataset import Dataset
    from gentropy.dataset.dataset_manager import (
        DatasetManager,
        DatasetName,
        DatasetDerivative,
    )
    from gentropy.dataset.study_locus import StudyLocus
    from gentropy.dataset.l2g_features.l2g_feature import L2GFeature


class FeatureFactory:
    """Factory class for creating features."""

    feature_registry: Mapping[L2GFeatureName, type[L2GFeature]] = {
        DistanceSentinelTssFeature.feature_name: DistanceSentinelTssFeature,
        DistanceSentinelTssNeighbourhoodFeature.feature_name: DistanceSentinelTssNeighbourhoodFeature,
        DistanceSentinelFootprintFeature.feature_name: DistanceSentinelFootprintFeature,
        DistanceSentinelFootprintNeighbourhoodFeature.feature_name: DistanceSentinelFootprintNeighbourhoodFeature,
        DistanceTssMeanFeature.feature_name: DistanceTssMeanFeature,
        DistanceTssMeanNeighbourhoodFeature.feature_name: DistanceTssMeanNeighbourhoodFeature,
        DistanceFootprintMeanFeature.feature_name: DistanceFootprintMeanFeature,
        DistanceFootprintMeanNeighbourhoodFeature.feature_name: DistanceFootprintMeanNeighbourhoodFeature,
        EQtlColocClppMaximumFeature.feature_name: EQtlColocClppMaximumFeature,
        EQtlColocClppMaximumNeighbourhoodFeature.feature_name: EQtlColocClppMaximumNeighbourhoodFeature,
        PQtlColocClppMaximumFeature.feature_name: PQtlColocClppMaximumFeature,
        PQtlColocClppMaximumNeighbourhoodFeature.feature_name: PQtlColocClppMaximumNeighbourhoodFeature,
        SQtlColocClppMaximumFeature.feature_name: SQtlColocClppMaximumFeature,
        SQtlColocClppMaximumNeighbourhoodFeature.feature_name: SQtlColocClppMaximumNeighbourhoodFeature,
        EQtlColocH4MaximumFeature.feature_name: EQtlColocH4MaximumFeature,
        EQtlColocH4MaximumNeighbourhoodFeature.feature_name: EQtlColocH4MaximumNeighbourhoodFeature,
        PQtlColocH4MaximumFeature.feature_name: PQtlColocH4MaximumFeature,
        PQtlColocH4MaximumNeighbourhoodFeature.feature_name: PQtlColocH4MaximumNeighbourhoodFeature,
        SQtlColocH4MaximumFeature.feature_name: SQtlColocH4MaximumFeature,
        SQtlColocH4MaximumNeighbourhoodFeature.feature_name: SQtlColocH4MaximumNeighbourhoodFeature,
        VepMeanFeature.feature_name: VepMeanFeature,
        VepMeanNeighbourhoodFeature.feature_name: VepMeanNeighbourhoodFeature,
        VepMaximumFeature.feature_name: VepMaximumFeature,
        VepMaximumNeighbourhoodFeature.feature_name: VepMaximumNeighbourhoodFeature,
        GeneCountFeature.feature_name: GeneCountFeature,
        ProteinGeneCountFeature.feature_name: ProteinGeneCountFeature,
        ProteinCodingFeature.feature_name: ProteinCodingFeature,
        CredibleSetConfidenceFeature.feature_name: CredibleSetConfidenceFeature,
    }

    def compute(
        self: FeatureFactory,
        study_loci_to_annotate: StudyLocus,
        feature_name: L2GFeatureName | str,
        dependency_registry: DatasetManager[DatasetDerivative],
    ) -> L2GFeature:
        """Instantiates feature class.

        Args:
            study_loci_to_annotate (StudyLocus): StudyLocus object used to build the feature.
            feature_name (L2GFeatureName | str): name of the feature
            features_input_loader (L2GFeatureInputLoader): Object that contais features input.
            dependency_registry (DatasetManager): Object to retrieve Datasets required to build the feature.

        Returns:
            L2GFeature: instantiated feature object

        Raises:
            L2GFeatureError: When provided incorrect feature name.
        """
        # Extract feature class and dependency type
        feature_name = L2GFeatureName(feature_name)
        feature_cls = self.feature_registry.get(feature_name)
        if not feature_cls:
            raise L2GFeatureError("Provided incorrect feature name")
        dependency_names: list[DatasetName] = feature_cls.dependency_names
        feature_dependencies: Mapping[DatasetName, Dataset] = {
            name: dependency_registry.get(name) for name in dependency_names
        }

        return feature_cls.compute(study_loci_to_annotate, feature_dependencies)
