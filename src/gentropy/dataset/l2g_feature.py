"""L2G Feature Dataset with a collection of methods that extract features from the gentropy datasets to be fed in L2G."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
from pyspark.sql import Window

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark_helpers import convert_from_wide_to_long
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.variant_index import VariantIndex

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
    from gentropy.dataset.study_locus import StudyLocus


@dataclass
class L2GFeature(Dataset, ABC):
    """Locus-to-gene feature dataset."""

    def __post_init__(
        self: L2GFeature,
        feature_dependency_type: Any = None,
        credible_set: StudyLocus | None = None,
    ) -> None:
        """Initializes a L2GFeature dataset. Any child class of L2GFeature must implement the `compute` method.

        Args:
            feature_dependency_type (Any): The dependency that the L2GFeature dataset depends on. Defaults to None.
            credible_set (StudyLocus | None): The credible set that the L2GFeature dataset is based on. Defaults to None.
        """
        super().__post_init__()
        self.feature_dependency_type = feature_dependency_type
        self.credible_set = credible_set

    @classmethod
    def get_schema(cls: type[L2GFeature]) -> StructType:
        """Provides the schema for the L2GFeature dataset.

        Returns:
            StructType: Schema for the L2GFeature dataset
        """
        return parse_spark_schema("l2g_feature.json")

    @classmethod
    @abstractmethod
    def compute(
        cls: type[L2GFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: Any,
    ) -> L2GFeature:
        """Computes the L2GFeature dataset.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (Any): The dependency that the L2GFeature class needs to compute the feature
        Returns:
            L2GFeature: a L2GFeature dataset

        Raises:
                NotImplementedError: This method must be implemented in the child classes
        """
        raise NotImplementedError("Must be implemented in the child classes")


def _common_colocalisation_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    colocalisation_method: str,
    colocalisation_metric: str,
    feature_name: str,
    qtl_type: str,
    *,
    colocalisation: Colocalisation,
    study_index: StudyIndex,
    study_locus: StudyLocus,
) -> DataFrame:
    """Wrapper to call the logic that creates a type of colocalisation features.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
        colocalisation_method (str): The colocalisation method to filter the data by
        colocalisation_metric (str): The colocalisation metric to use
        feature_name (str): The name of the feature to create
        qtl_type (str): The type of QTL to filter the data by
        colocalisation (Colocalisation): Dataset with the colocalisation results
        study_index (StudyIndex): Study index to fetch study type and gene
        study_locus (StudyLocus): Study locus to traverse between colocalisation and study index

    Returns:
        DataFrame: Feature annotation in long format with the columns: studyLocusId, geneId, featureName, featureValue
    """
    joining_cols = (
        ["studyLocusId", "geneId"]
        if isinstance(study_loci_to_annotate, L2GGoldStandard)
        else ["studyLocusId"]
    )
    return (
        study_loci_to_annotate.df.join(
            colocalisation.extract_maximum_coloc_probability_per_region_and_gene(
                study_locus,
                study_index,
                filter_by_colocalisation_method=colocalisation_method,
                filter_by_qtl=qtl_type,
            ),
            on=joining_cols,
        )
        .selectExpr(
            "studyLocusId",
            "geneId",
            f"{colocalisation_metric} as {feature_name}",
        )
        .distinct()
    )


def _common_neighbourhood_colocalisation_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    colocalisation_method: str,
    colocalisation_metric: str,
    feature_name: str,
    qtl_type: str,
    *,
    colocalisation: Colocalisation,
    study_index: StudyIndex,
    study_locus: StudyLocus,
) -> DataFrame:
    """Wrapper to call the logic that creates a type of colocalisation features.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
        colocalisation_method (str): The colocalisation method to filter the data by
        colocalisation_metric (str): The colocalisation metric to use
        feature_name (str): The name of the feature to create
        qtl_type (str): The type of QTL to filter the data by
        colocalisation (Colocalisation): Dataset with the colocalisation results
        study_index (StudyIndex): Study index to fetch study type and gene
        study_locus (StudyLocus): Study locus to traverse between colocalisation and study index

    Returns:
        DataFrame: Feature annotation in long format with the columns: studyLocusId, geneId, featureName, featureValue
    """
    # First maximum colocalisation score for each studylocus, gene
    local_feature_name = feature_name.replace("Neighbourhood", "")
    local_max = _common_colocalisation_feature_logic(
        study_loci_to_annotate,
        colocalisation_method,
        colocalisation_metric,
        local_feature_name,
        qtl_type,
        colocalisation=colocalisation,
        study_index=study_index,
        study_locus=study_locus,
    )
    return (
        # Then compute maximum score in the vicinity (feature will be the same for any gene associated with a studyLocus)
        local_max.withColumn(
            "regional_maximum",
            f.max(local_feature_name).over(Window.partitionBy("studyLocusId")),
        )
        .withColumn(feature_name, f.col("regional_maximum") - f.col(local_feature_name))
        .drop("regional_maximum")
    )


class EQtlColocClppMaximumFeature(L2GFeature):
    """Max CLPP for each (study, locus, gene) aggregating over all eQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus]
    feature_name = "eQtlColocClppMaximum"

    @classmethod
    def compute(
        cls: type[EQtlColocClppMaximumFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> EQtlColocClppMaximumFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dictionary with the dependencies required. They are passed as keyword arguments.

        Returns:
            EQtlColocClppMaximumFeature: Feature dataset
        """
        colocalisation_method = "ECaviar"
        colocalisation_metric = "clpp"
        qtl_type = "eqtl"

        return cls(
            _df=convert_from_wide_to_long(
                _common_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class EQtlColocClppMaximumNeighbourhoodFeature(L2GFeature):
    """Max CLPP for each (study, locus) aggregating over all eQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus]
    feature_name = "eQtlColocClppMaximumNeighbourhood"

    @classmethod
    def compute(
        cls: type[EQtlColocClppMaximumNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> EQtlColocClppMaximumNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dictionary with the dependencies required. They are passed as keyword arguments.

        Returns:
            EQtlColocClppMaximumNeighbourhoodFeature: Feature dataset
        """
        colocalisation_method = "ECaviar"
        colocalisation_metric = "clpp"
        qtl_type = "eqtl"

        return cls(
            _df=convert_from_wide_to_long(
                _common_neighbourhood_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class PQtlColocClppMaximumFeature(L2GFeature):
    """Max CLPP for each (study, locus, gene) aggregating over all pQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus]
    feature_name = "pQtlColocClppMaximum"

    @classmethod
    def compute(
        cls: type[PQtlColocClppMaximumFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> PQtlColocClppMaximumFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            PQtlColocClppMaximumFeature: Feature dataset
        """
        colocalisation_method = "ECaviar"
        colocalisation_metric = "clpp"
        qtl_type = "pqtl"
        return cls(
            _df=convert_from_wide_to_long(
                _common_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class PQtlColocClppMaximumNeighbourhoodFeature(L2GFeature):
    """Max CLPP for each (study, locus, gene) aggregating over all pQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus]
    feature_name = "pQtlColocClppMaximumNeighbourhood"

    @classmethod
    def compute(
        cls: type[PQtlColocClppMaximumNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> PQtlColocClppMaximumNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            PQtlColocClppMaximumNeighbourhoodFeature: Feature dataset
        """
        colocalisation_method = "ECaviar"
        colocalisation_metric = "clpp"
        qtl_type = "pqtl"
        return cls(
            _df=convert_from_wide_to_long(
                _common_neighbourhood_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class SQtlColocClppMaximumFeature(L2GFeature):
    """Max CLPP for each (study, locus, gene) aggregating over all sQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus]
    feature_name = "sQtlColocClppMaximum"

    @classmethod
    def compute(
        cls: type[SQtlColocClppMaximumFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> SQtlColocClppMaximumFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            SQtlColocClppMaximumFeature: Feature dataset
        """
        colocalisation_method = "ECaviar"
        colocalisation_metric = "clpp"
        qtl_type = "sqtl"
        return cls(
            _df=convert_from_wide_to_long(
                _common_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class SQtlColocClppMaximumNeighbourhoodFeature(L2GFeature):
    """Max CLPP for each (study, locus, gene) aggregating over all sQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus]
    feature_name = "sQtlColocClppMaximumNeighbourhood"

    @classmethod
    def compute(
        cls: type[SQtlColocClppMaximumNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> SQtlColocClppMaximumNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            SQtlColocClppMaximumNeighbourhoodFeature: Feature dataset
        """
        colocalisation_method = "ECaviar"
        colocalisation_metric = "clpp"
        qtl_type = "sqtl"
        return cls(
            _df=convert_from_wide_to_long(
                _common_neighbourhood_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class TuQtlColocClppMaximumFeature(L2GFeature):
    """Max CLPP for each (study, locus, gene) aggregating over all tuQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus]
    feature_name = "tuQtlColocClppMaximum"

    @classmethod
    def compute(
        cls: type[TuQtlColocClppMaximumFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> TuQtlColocClppMaximumFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            TuQtlColocClppMaximumFeature: Feature dataset
        """
        colocalisation_method = "ECaviar"
        colocalisation_metric = "clpp"
        qtl_type = "tuqtl"
        return cls(
            _df=convert_from_wide_to_long(
                _common_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class TuQtlColocClppMaximumNeighbourhoodFeature(L2GFeature):
    """Max CLPP for each (study, locus) aggregating over all tuQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus]
    feature_name = "tuQtlColocClppMaximumNeighbourhood"

    @classmethod
    def compute(
        cls: type[TuQtlColocClppMaximumNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> TuQtlColocClppMaximumNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            TuQtlColocClppMaximumNeighbourhoodFeature: Feature dataset
        """
        colocalisation_method = "ECaviar"
        colocalisation_metric = "clpp"
        qtl_type = "tuqtl"
        return cls(
            _df=convert_from_wide_to_long(
                _common_neighbourhood_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class EQtlColocH4MaximumFeature(L2GFeature):
    """Max H4 for each (study, locus, gene) aggregating over all eQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus]
    feature_name = "eQtlColocH4Maximum"

    @classmethod
    def compute(
        cls: type[EQtlColocH4MaximumFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> EQtlColocH4MaximumFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            EQtlColocH4MaximumFeature: Feature dataset
        """
        colocalisation_method = "Coloc"
        colocalisation_metric = "h4"
        qtl_type = "eqtl"
        return cls(
            _df=convert_from_wide_to_long(
                _common_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class EQtlColocH4MaximumNeighbourhoodFeature(L2GFeature):
    """Max H4 for each (study, locus) aggregating over all eQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus]
    feature_name = "eQtlColocH4MaximumNeighbourhood"

    @classmethod
    def compute(
        cls: type[EQtlColocH4MaximumNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> EQtlColocH4MaximumNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            EQtlColocH4MaximumNeighbourhoodFeature: Feature dataset
        """
        colocalisation_method = "Coloc"
        colocalisation_metric = "h4"
        qtl_type = "eqtl"
        return cls(
            _df=convert_from_wide_to_long(
                _common_neighbourhood_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class PQtlColocH4MaximumFeature(L2GFeature):
    """Max H4 for each (study, locus, gene) aggregating over all pQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus]
    feature_name = "pQtlColocH4Maximum"

    @classmethod
    def compute(
        cls: type[PQtlColocH4MaximumFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> PQtlColocH4MaximumFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            PQtlColocH4MaximumFeature: Feature dataset
        """
        colocalisation_method = "Coloc"
        colocalisation_metric = "h4"
        qtl_type = "pqtl"
        return cls(
            _df=convert_from_wide_to_long(
                _common_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class PQtlColocH4MaximumNeighbourhoodFeature(L2GFeature):
    """Max H4 for each (study, locus) aggregating over all pQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus]
    feature_name = "pQtlColocH4MaximumNeighbourhood"

    @classmethod
    def compute(
        cls: type[PQtlColocH4MaximumNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> PQtlColocH4MaximumNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            PQtlColocH4MaximumNeighbourhoodFeature: Feature dataset
        """
        colocalisation_method = "Coloc"
        colocalisation_metric = "h4"
        qtl_type = "pqtl"
        return cls(
            _df=convert_from_wide_to_long(
                _common_neighbourhood_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class SQtlColocH4MaximumFeature(L2GFeature):
    """Max H4 for each (study, locus, gene) aggregating over all sQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus]
    feature_name = "sQtlColocH4Maximum"

    @classmethod
    def compute(
        cls: type[SQtlColocH4MaximumFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> SQtlColocH4MaximumFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            SQtlColocH4MaximumFeature: Feature dataset
        """
        colocalisation_method = "Coloc"
        colocalisation_metric = "h4"
        qtl_type = "sqtl"
        return cls(
            _df=convert_from_wide_to_long(
                _common_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class SQtlColocH4MaximumNeighbourhoodFeature(L2GFeature):
    """Max H4 for each (study, locus) aggregating over all sQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex]
    feature_name = "sQtlColocH4MaximumNeighbourhood"

    @classmethod
    def compute(
        cls: type[SQtlColocH4MaximumNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> SQtlColocH4MaximumNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            SQtlColocH4MaximumNeighbourhoodFeature: Feature dataset
        """
        colocalisation_method = "Coloc"
        colocalisation_metric = "h4"
        qtl_type = "sqtl"
        return cls(
            _df=convert_from_wide_to_long(
                _common_neighbourhood_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class TuQtlColocH4MaximumFeature(L2GFeature):
    """Max H4 for each (study, locus, gene) aggregating over all tuQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus]
    feature_name = "tuQtlColocH4Maximum"

    @classmethod
    def compute(
        cls: type[TuQtlColocH4MaximumFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> TuQtlColocH4MaximumFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            TuQtlColocH4MaximumFeature: Feature dataset
        """
        colocalisation_method = "Coloc"
        colocalisation_metric = "h4"
        qtl_type = "tuqtl"
        return cls(
            _df=convert_from_wide_to_long(
                _common_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class TuQtlColocH4MaximumNeighbourhoodFeature(L2GFeature):
    """Max H4 for each (study, locus) aggregating over all tuQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus]
    feature_name = "tuQtlColocH4MaximumNeighbourhood"

    @classmethod
    def compute(
        cls: type[TuQtlColocH4MaximumNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> TuQtlColocH4MaximumNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            TuQtlColocH4MaximumNeighbourhoodFeature: Feature dataset
        """
        colocalisation_method = "Coloc"
        colocalisation_metric = "h4"
        qtl_type = "tuqtl"
        return cls(
            _df=convert_from_wide_to_long(
                _common_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_type,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


def _common_distance_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    *,
    variant_index: VariantIndex,
    feature_name: str,
    distance_type: str,
    agg_expr: Column,
) -> DataFrame:
    """Computes the feature.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
        variant_index (VariantIndex): The dataset containing distance to gene information
        feature_name (str): The name of the feature
        distance_type (str): The type of distance to gene
        agg_expr (Column): The expression that aggregate distances into a specific way to define the feature

    Returns:
            DataFrame: Feature dataset
    """
    distances_dataset = variant_index.get_distance_to_gene(distance_type=distance_type)
    return (
        study_loci_to_annotate.df.withColumn("variantInLocus", f.explode_outer("locus"))
        .select(
            "studyLocusId",
            f.col("variantInLocus.variantId").alias("variantInLocusId"),
            f.col("variantInLocus.posteriorProbability").alias(
                "variantInLocusPosteriorProbability"
            ),
        )
        .join(
            distances_dataset.withColumnRenamed(
                "variantId", "variantInLocusId"
            ).withColumnRenamed("targetId", "geneId"),
            on="variantInLocusId",
            how="inner",
        )
        .withColumn(
            "weightedDistance",
            f.col(distance_type) * f.col("variantInLocusPosteriorProbability"),
        )
        .groupBy("studyLocusId", "geneId")
        .agg(agg_expr.alias(feature_name))
    )


def _common_neighbourhood_distance_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    *,
    variant_index: VariantIndex,
    feature_name: str,
    distance_type: str,
    agg_expr: Column,
) -> DataFrame:
    """Calculate the neighbourhood distance feature.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
        variant_index (VariantIndex): The dataset containing distance to gene information
        feature_name (str): The name of the feature
        distance_type (str): The type of distance to gene
        agg_expr (Column): The expression that aggregate distances into a specific way to define the feature

    Returns:
            DataFrame: Feature dataset
    """
    local_feature_name = feature_name.replace("Neighbourhood", "")
    # First compute mean distances to a gene
    local_min = _common_distance_feature_logic(
        study_loci_to_annotate,
        feature_name=local_feature_name,
        distance_type=distance_type,
        agg_expr=agg_expr,
        variant_index=variant_index,
    )
    return (
        # Then compute minimum distance in the vicinity (feature will be the same for any gene associated with a studyLocus)
        local_min.withColumn(
            "regional_minimum",
            f.min(local_feature_name).over(Window.partitionBy("studyLocusId")),
        )
        .withColumn(feature_name, f.col("regional_minimum") - f.col(local_feature_name))
        .drop("regional_minimum")
    )


class DistanceTssMeanFeature(L2GFeature):
    """Average distance of all tagging variants to gene TSS."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "distanceTssMean"

    @classmethod
    def compute(
        cls: type[DistanceTssMeanFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceTssMeanFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceTssMeanFeature: Feature dataset
        """
        agg_expr = f.mean("weightedDistance")
        distance_type = "distanceFromTss"
        return cls(
            _df=convert_from_wide_to_long(
                _common_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class DistanceTssMeanNeighbourhoodFeature(L2GFeature):
    """Minimum mean distance to TSS for all genes in the vicinity of a studyLocus."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "distanceTssMeanNeighbourhood"

    @classmethod
    def compute(
        cls: type[DistanceTssMeanNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceTssMeanNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceTssMeanNeighbourhoodFeature: Feature dataset
        """
        agg_expr = f.mean("weightedDistance")
        distance_type = "distanceFromTss"
        return cls(
            _df=convert_from_wide_to_long(
                _common_neighbourhood_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class DistanceTssMinimumFeature(L2GFeature):
    """Minimum distance of all tagging variants to gene TSS."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "distanceTssMinimum"

    @classmethod
    def compute(
        cls: type[DistanceTssMinimumFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceTssMinimumFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceTssMinimumFeature: Feature dataset
        """
        agg_expr = f.mean("weightedDistance")
        distance_type = "distanceFromTss"
        return cls(
            _df=convert_from_wide_to_long(
                _common_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class DistanceTssMinimumNeighbourhoodFeature(L2GFeature):
    """Minimum minimum distance to TSS for all genes in the vicinity of a studyLocus."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "distanceTssMinimumNeighbourhood"

    @classmethod
    def compute(
        cls: type[DistanceTssMinimumNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceTssMinimumNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceTssMinimumNeighbourhoodFeature: Feature dataset
        """
        agg_expr = f.min("weightedDistance")
        distance_type = "distanceFromTss"
        return cls(
            _df=convert_from_wide_to_long(
                _common_neighbourhood_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class DistanceFootprintMeanFeature(L2GFeature):
    """Average distance of all tagging variants to the footprint of a gene."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "distanceFootprintMean"

    @classmethod
    def compute(
        cls: type[DistanceFootprintMeanFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceFootprintMeanFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceFootprintMeanFeature: Feature dataset
        """
        agg_expr = f.mean("weightedDistance")
        distance_type = "distanceFromFootprint"
        return cls(
            _df=convert_from_wide_to_long(
                _common_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class DistanceFootprintMeanNeighbourhoodFeature(L2GFeature):
    """Minimum mean distance to footprint for all genes in the vicinity of a studyLocus."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "distanceFootprintMeanNeighbourhood"

    @classmethod
    def compute(
        cls: type[DistanceFootprintMeanNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceFootprintMeanNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceFootprintMeanNeighbourhoodFeature: Feature dataset
        """
        agg_expr = f.mean("weightedDistance")
        distance_type = "distanceFromFootprint"
        return cls(
            _df=convert_from_wide_to_long(
                _common_neighbourhood_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class DistanceFootprintMinimumFeature(L2GFeature):
    """Minimum distance of all tagging variants to the footprint of a gene."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "DistanceFootprintMinimum"

    @classmethod
    def compute(
        cls: type[DistanceFootprintMinimumFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceFootprintMinimumFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceFootprintMinimumFeature: Feature dataset
        """
        agg_expr = f.mean("weightedDistance")
        distance_type = "distanceFromFootprint"
        return cls(
            _df=convert_from_wide_to_long(
                _common_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class DistanceFootprintMinimumNeighbourhoodFeature(L2GFeature):
    """Minimum minimum distance to footprint for all genes in the vicinity of a studyLocus."""

    fill_na_value = 500_000
    feature_dependency_type = VariantIndex
    feature_name = "distanceFootprintMinimumNeighbourhood"

    @classmethod
    def compute(
        cls: type[DistanceFootprintMinimumNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> DistanceFootprintMinimumNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset that contains the distance information

        Returns:
            DistanceFootprintMinimumNeighbourhoodFeature: Feature dataset
        """
        agg_expr = f.min("weightedDistance")
        distance_type = "distanceFromFootprint"
        return cls(
            _df=convert_from_wide_to_long(
                _common_neighbourhood_distance_feature_logic(
                    study_loci_to_annotate,
                    feature_name=cls.feature_name,
                    distance_type=distance_type,
                    agg_expr=agg_expr,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )
