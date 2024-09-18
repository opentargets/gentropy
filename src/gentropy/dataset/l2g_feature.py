"""L2G Feature Dataset with a collection of methods that extract features from the gentropy datasets to be fed in L2G."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark_helpers import convert_from_wide_to_long
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.v2g import V2G

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
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
        """Initializes a L2GFeature dataset.

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
        """
        pass


def _common_colocalisation_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    colocalisation_method: str,
    colocalisation_metric: str,
    feature_name: str,
    qtl_type: str,
    *,
    colocalisation: Colocalisation,
    study_index: StudyIndex,
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

    Returns:
        DataFrame: Feature annotation in long format with the columns: studyLocusId, geneId, featureName, featureValue
    """
    return convert_from_wide_to_long(
        colocalisation.extract_maximum_coloc_probability_per_region_and_gene(
            study_loci_to_annotate,
            study_index,
            filter_by_colocalisation_method=colocalisation_method,
            filter_by_qtl=qtl_type,
        ).selectExpr(
            "studyLocusId",
            "geneId",
            f"{colocalisation_metric} as {feature_name}",
        ),
        id_vars=("studyLocusId", "geneId"),
        var_name="featureName",
        value_name="featureValue",
    )


class EQtlColocClppMaximumFeature(L2GFeature):
    """Max CLPP for each (study, locus, gene) aggregating over all eQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex]
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
            _df=_common_colocalisation_feature_logic(
                study_loci_to_annotate,
                colocalisation_method,
                colocalisation_metric,
                cls.feature_name,
                qtl_type,
                **feature_dependency,
            ),
            _schema=cls.get_schema(),
        )


class PQtlColocClppMaximumFeature(L2GFeature):
    """Max CLPP for each (study, locus, gene) aggregating over all pQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex]
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
            _df=_common_colocalisation_feature_logic(
                study_loci_to_annotate,
                colocalisation_method,
                colocalisation_metric,
                cls.feature_name,
                qtl_type,
                **feature_dependency,
            ),
            _schema=cls.get_schema(),
        )


class SQtlColocClppMaximumFeature(L2GFeature):
    """Max CLPP for each (study, locus, gene) aggregating over all sQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex]
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
            _df=_common_colocalisation_feature_logic(
                study_loci_to_annotate,
                colocalisation_method,
                colocalisation_metric,
                cls.feature_name,
                qtl_type,
                **feature_dependency,
            ),
            _schema=cls.get_schema(),
        )


class TuQtlColocClppMaximumFeature(L2GFeature):
    """Max CLPP for each (study, locus, gene) aggregating over all tuQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex]
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
            _df=_common_colocalisation_feature_logic(
                study_loci_to_annotate,
                colocalisation_method,
                colocalisation_metric,
                cls.feature_name,
                qtl_type,
                **feature_dependency,
            ),
            _schema=cls.get_schema(),
        )


class EQtlColocH4MaximumFeature(L2GFeature):
    """Max CLPP for each (study, locus, gene) aggregating over all eQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex]
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
            _df=_common_colocalisation_feature_logic(
                study_loci_to_annotate,
                colocalisation_method,
                colocalisation_metric,
                cls.feature_name,
                qtl_type,
                **feature_dependency,
            ),
            _schema=cls.get_schema(),
        )


class PQtlColocH4MaximumFeature(L2GFeature):
    """Max CLPP for each (study, locus, gene) aggregating over all pQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex]
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
            _df=_common_colocalisation_feature_logic(
                study_loci_to_annotate,
                colocalisation_method,
                colocalisation_metric,
                cls.feature_name,
                qtl_type,
                **feature_dependency,
            ),
            _schema=cls.get_schema(),
        )


class SQtlColocH4MaximumFeature(L2GFeature):
    """Max CLPP for each (study, locus, gene) aggregating over all sQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex]
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
            _df=_common_colocalisation_feature_logic(
                study_loci_to_annotate,
                colocalisation_method,
                colocalisation_metric,
                cls.feature_name,
                qtl_type,
                **feature_dependency,
            ),
            _schema=cls.get_schema(),
        )


class TuQtlColocH4MaximumFeature(L2GFeature):
    """Max H4 for each (study, locus, gene) aggregating over all tuQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex]
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
            _df=_common_colocalisation_feature_logic(
                study_loci_to_annotate,
                colocalisation_method,
                colocalisation_metric,
                cls.feature_name,
                qtl_type,
                **feature_dependency,
            ),
            _schema=cls.get_schema(),
        )


class DistanceTssMinimumFeature(L2GFeature):
    """Minimum distance of all tagging variants to gene TSS."""

    @classmethod
    def compute(
        cls: type[DistanceTssMinimumFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: V2G,
    ) -> L2GFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (V2G): Dataset that contains the distance information

        Returns:
                L2GFeature: Feature dataset

        Raises:
            NotImplementedError: Not implemented
        """
        raise NotImplementedError


class DistanceTssMeanFeature(L2GFeature):
    """Average distance of all tagging variants to gene TSS.

    NOTE: to be rewritten taking variant index as input
    """

    fill_na_value = 500_000
    feature_dependency_type = V2G

    @classmethod
    def compute(
        cls: type[DistanceTssMeanFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: V2G,
    ) -> DistanceTssMeanFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (V2G): Dataset that contains the distance information

        Returns:
            DistanceTssMeanFeature: Feature dataset
        """
        agg_expr = f.mean("weightedScore").alias("distanceTssMean")
        # Everything but expresion is common logic
        v2g = feature_dependency.df.filter(f.col("datasourceId") == "canonical_tss")
        wide_df = (
            study_loci_to_annotate.df.withColumn(
                "variantInLocus", f.explode_outer("locus")
            )
            .select(
                "studyLocusId",
                f.col("variantInLocus.variantId").alias("variantInLocusId"),
                f.col("variantInLocus.posteriorProbability").alias(
                    "variantInLocusPosteriorProbability"
                ),
            )
            .join(
                v2g.selectExpr("variantId as variantInLocusId", "geneId", "score"),
                on="variantInLocusId",
                how="inner",
            )
            .withColumn(
                "weightedScore",
                f.col("score") * f.col("variantInLocusPosteriorProbability"),
            )
            .groupBy("studyLocusId", "geneId")
            .agg(agg_expr)
        )
        return cls(
            _df=convert_from_wide_to_long(
                wide_df,
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )
