"""Collection of methods that extract features from the colocalisation datasets."""

from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window

from gentropy.common.spark import convert_from_wide_to_long
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.target_index import TargetIndex
from gentropy.dataset.variant_index import VariantIndex
from gentropy.method.colocalisation import ColocalisationMethod

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def extract_maximum_coloc_probability_per_region_and_gene(
    coloc: Colocalisation,
    study_locus: StudyLocus,
    study_index: StudyIndex,
    *,
    filter_by_colocalisation_method: str,
    filter_by_qtls: list[str] | None = None,
) -> DataFrame:
    """Get maximum colocalisation probability for a (studyLocus, gene) window.

    Args:
        coloc (Colocalisation): Colocalisation dataset to extract the information from
        study_locus (StudyLocus): Dataset containing study loci to filter the colocalisation dataset on and the geneId linked to the region
        study_index (StudyIndex): Study index to use to get study metadata
        filter_by_colocalisation_method (str): optional filter to apply on the colocalisation dataset
        filter_by_qtls (list[str] | None): optional filter to apply on the colocalisation dataset

    Returns:
        DataFrame: table with the maximum colocalisation scores for the provided study loci

    Raises:
        InvalidColocalisationMethodError: if `filter_by_colocalisation_method` is not a valid colocalisation method
        ValueError: if `filter_by_qtls` is not in the list of valid QTL types
    """
    from gentropy.common.spark import get_record_with_maximum_value
    from gentropy.datasource.eqtl_catalogue import StudyType

    valid_qtls = {v.value for v in StudyType}

    # NOTE: since we build features based on Coloc and ECaviar results, we do not expect to have
    # a situation when the `filter_by_colocalisation_method` points to ColocPIP or ColocPIPECaviar.
    cc = ColocalisationMethod.get_method_class(filter_by_colocalisation_method)
    # Get the colocalisation method metric -> this will be either (COLOC => h4, ECAVIAR => CLPP)
    metric = cc.METHOD_METRICS[0]  # First metric in ColocalisationMethod ( h4 or CLPP )
    # Prepare the list of colocalisation methods that contain expected metric
    allowed_coloc_methods = {
        c.lower() for c in ColocalisationMethod.get_method_names_for_metric(metric)
    }
    # Since colocalisation dataset can have multiple values in `colocalisationMethod` column,
    # we need to filter the dataset for the subset of rows that corresponds to the method
    # that implements the specific metric.
    coloc_filtering_expr = [
        f.col("rightGeneId").isNotNull(),
        (f.lower("colocalisationMethod").isin(allowed_coloc_methods)),
    ]
    if filter_by_qtls:
        _required_study_types = set(map(str.lower, filter_by_qtls))
        if not _required_study_types.issubset(valid_qtls):
            raise ValueError(f"There are no studies with some of QTLs {filter_by_qtls}")
        coloc_filtering_expr.append(
            f.lower("rightStudyType").isin(_required_study_types)
        )

    filtered_colocalisation = (
        # Bring rightStudyType and rightGeneId and filter by rows where the gene is null,
        # which is equivalent to filtering studyloci from gwas on the right side
        coloc.append_study_metadata(
            study_locus,
            study_index,
            metadata_cols=["geneId", "studyType"],
            colocalisation_side="right",
        )
        # it also filters based on method and qtl type
        .filter(reduce(lambda a, b: a & b, coloc_filtering_expr))
        # and filters colocalisation results to only include the subset of studylocus that contains gwas studylocusid
        .join(
            study_locus.df.selectExpr("studyLocusId as leftStudyLocusId"),
            "leftStudyLocusId",
        )
    )

    return get_record_with_maximum_value(
        filtered_colocalisation.withColumnRenamed(
            "leftStudyLocusId", "studyLocusId"
        ).withColumnRenamed("rightGeneId", "geneId"),
        ["studyLocusId", "geneId"],
        metric,
    )


def common_colocalisation_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    colocalisation_method: str,
    colocalisation_metric: str,
    feature_name: str,
    qtl_types: list[str],
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
        qtl_types (list[str]): The types of QTL to filter the data by
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
            # Remove colocalisation with trans QTLs
            # Extract maximum colocalisation probability per region and gene
            extract_maximum_coloc_probability_per_region_and_gene(
                coloc=colocalisation.drop_trans_effects(study_locus),
                study_locus=study_locus,
                study_index=study_index,
                filter_by_colocalisation_method=colocalisation_method,
                filter_by_qtls=qtl_types,
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


def extend_missing_colocalisation_to_neighbourhood_genes(
    feature_name: str,
    local_features: DataFrame,
    variant_index: VariantIndex,
    target_index: TargetIndex,
    study_locus: StudyLocus,
) -> DataFrame:
    """This function creates an artificial dataset of features that represents the missing colocalisation to the neighbourhood genes.

    Args:
        feature_name (str): The name of the feature to extend
        local_features (DataFrame): The dataframe of features to extend
        variant_index (VariantIndex): Variant index containing all variant/gene relationships
        target_index (TargetIndex): Target index to fetch the gene information
        study_locus (StudyLocus): Study locus to traverse between colocalisation and variant index

    Returns:
        DataFrame: Dataframe of features that include genes in the neighbourhood not present in the colocalisation results. For these genes, the feature value is set to 0.
    """
    coding_variant_gene_lut = (
        variant_index.df.select(
            "variantId", f.explode("transcriptConsequences").alias("tc")
        )
        .select(f.col("tc.targetId").alias("geneId"), "variantId")
        .join(
            target_index.df.select(f.col("id").alias("geneId"), "biotype"),
            "geneId",
            "left",
        )
        .filter(f.col("biotype") == "protein_coding")
        .drop("biotype")
        .distinct()
    )
    local_features_w_variant = local_features.join(
        study_locus.df.select("studyLocusId", "variantId"), "studyLocusId"
    )
    return (
        # Get the genes that are not present in the colocalisation results
        coding_variant_gene_lut.join(
            local_features_w_variant, ["variantId", "geneId"], "left_anti"
        )
        # We now link the missing variant/gene to the study locus from the original dataframe
        .join(
            local_features_w_variant.select("studyLocusId", "variantId").distinct(),
            "variantId",
        )
        .drop("variantId")
        # Fill the information for missing genes with 0
        .withColumn(feature_name, f.lit(0.0))
    )


def common_neighbourhood_colocalisation_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    colocalisation_method: str,
    colocalisation_metric: str,
    feature_name: str,
    qtl_types: list[str],
    *,
    colocalisation: Colocalisation,
    study_index: StudyIndex,
    target_index: TargetIndex,
    study_locus: StudyLocus,
    variant_index: VariantIndex,
) -> DataFrame:
    """Wrapper to call the logic that creates a type of colocalisation features.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
        colocalisation_method (str): The colocalisation method to filter the data by
        colocalisation_metric (str): The colocalisation metric to use
        feature_name (str): The name of the feature to create
        qtl_types (list[str]): The types of QTL to filter the data by
        colocalisation (Colocalisation): Dataset with the colocalisation results
        study_index (StudyIndex): Study index to fetch study type and gene
        target_index (TargetIndex): Target index to add gene type
        study_locus (StudyLocus): Study locus to traverse between colocalisation and study index
        variant_index (VariantIndex): Variant index to annotate all overlapping genes

    Returns:
        DataFrame: Feature annotation in long format with the columns: studyLocusId, geneId, featureName, featureValue
    """
    # First maximum colocalisation score for each studylocus, gene
    local_feature_name = feature_name.replace("Neighbourhood", "")
    local_max = common_colocalisation_feature_logic(
        study_loci_to_annotate,
        colocalisation_method,
        colocalisation_metric,
        local_feature_name,
        qtl_types,
        colocalisation=colocalisation,
        study_index=study_index,
        study_locus=study_locus,
    )
    extended_local_max = local_max.unionByName(
        extend_missing_colocalisation_to_neighbourhood_genes(
            local_feature_name,
            local_max,
            variant_index,
            target_index,
            study_locus,
        )
    )
    return (
        extended_local_max.join(
            # Compute average score in the vicinity (feature will be the same for any gene associated with a studyLocus)
            # (non protein coding genes in the vicinity are excluded see #3552)
            target_index.df.filter(f.col("biotype") == "protein_coding").select(
                f.col("id").alias("geneId")
            ),
            "geneId",
            "inner",
        )
        .withColumn(
            "regional_max",
            f.max(local_feature_name).over(Window.partitionBy("studyLocusId")),
        )
        .withColumn(
            feature_name,
            f.when(
                (f.col("regional_max").isNotNull()) & (f.col("regional_max") != 0.0),
                f.col(local_feature_name)
                / f.coalesce(f.col("regional_max"), f.lit(0.0)),
            ).otherwise(f.lit(0.0)),
        )
        .drop("regional_max", local_feature_name)
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
        qtl_type = ["eqtl", "sceqtl"]

        return cls(
            _df=convert_from_wide_to_long(
                common_colocalisation_feature_logic(
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

    feature_dependency_type = [
        Colocalisation,
        StudyIndex,
        TargetIndex,
        StudyLocus,
        VariantIndex,
    ]
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
        qtl_type = ["eqtl", "sceqtl"]

        return cls(
            _df=convert_from_wide_to_long(
                common_neighbourhood_colocalisation_feature_logic(
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
        qtl_type = ["pqtl"]
        return cls(
            _df=convert_from_wide_to_long(
                common_colocalisation_feature_logic(
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

    feature_dependency_type = [
        Colocalisation,
        StudyIndex,
        TargetIndex,
        StudyLocus,
        VariantIndex,
    ]
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
        qtl_type = ["pqtl"]
        return cls(
            _df=convert_from_wide_to_long(
                common_neighbourhood_colocalisation_feature_logic(
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
        qtl_types = ["sqtl", "tuqtl", "scsqtl", "sctuqtl"]
        return cls(
            _df=convert_from_wide_to_long(
                common_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_types,
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

    feature_dependency_type = [
        Colocalisation,
        StudyIndex,
        TargetIndex,
        StudyLocus,
        VariantIndex,
    ]
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
        qtl_types = ["sqtl", "tuqtl", "scsqtl", "sctuqtl"]
        return cls(
            _df=convert_from_wide_to_long(
                common_neighbourhood_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_types,
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
        qtl_type = ["eqtl", "sceqtl"]
        return cls(
            _df=convert_from_wide_to_long(
                common_colocalisation_feature_logic(
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

    feature_dependency_type = [
        Colocalisation,
        StudyIndex,
        TargetIndex,
        StudyLocus,
        VariantIndex,
    ]
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
        qtl_type = ["eqtl", "sceqtl"]
        return cls(
            _df=convert_from_wide_to_long(
                common_neighbourhood_colocalisation_feature_logic(
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
        qtl_type = ["pqtl"]
        return cls(
            _df=convert_from_wide_to_long(
                common_colocalisation_feature_logic(
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

    feature_dependency_type = [
        Colocalisation,
        StudyIndex,
        TargetIndex,
        StudyLocus,
        VariantIndex,
    ]
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
        qtl_type = ["pqtl"]
        return cls(
            _df=convert_from_wide_to_long(
                common_neighbourhood_colocalisation_feature_logic(
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
        qtl_types = ["sqtl", "tuqtl", "scsqtl", "sctuqtl"]
        return cls(
            _df=convert_from_wide_to_long(
                common_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_types,
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

    feature_dependency_type = [
        Colocalisation,
        StudyIndex,
        TargetIndex,
        StudyLocus,
        VariantIndex,
    ]
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
        qtl_types = ["sqtl", "tuqtl", "scsqtl", "sctuqtl"]
        return cls(
            _df=convert_from_wide_to_long(
                common_neighbourhood_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    qtl_types,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


def common_trans_pqtl_colocalisation_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    colocalisation_method: str,
    colocalisation_metric: str,
    feature_name: str,
    *,
    colocalisation: Colocalisation,
    study_index: StudyIndex,
    study_locus: StudyLocus,
    interactions: DataFrame,
    target_index: TargetIndex,
    string_threshold: float = 0.75,
    intact_threshold: float = 0.42,
    delta: int = 500_000,
) -> DataFrame:
    """Wrapper to call the logic that creates trans-pQTL colocalisation features.

    For each GWAS locus, the feature fires for a gene (targetB) when:
    1. The GWAS locus colocalises with a trans-pQTL study locus (right side, isTransQtl=True).
    2. The trans-pQTL gene (transPQTLGeneId) physically interacts with targetB via any supported
       interaction source: STRING (scoring >= string_threshold), IntAct (scoring >= intact_threshold),
       or any other source without a score threshold (e.g. Signor, Reactome).
    3. targetB is located on the same chromosome as the GWAS signal and within `delta` bp of it.

    Feature value = max colocalisation score across all qualifying trans-pQTL colocalisations.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
        colocalisation_method (str): The colocalisation method to filter the data by
        colocalisation_metric (str): The colocalisation metric to use
        feature_name (str): The name of the feature to create
        colocalisation (Colocalisation): Dataset with the colocalisation results
        study_index (StudyIndex): Study index to fetch study type and gene
        study_locus (StudyLocus): Study locus to traverse between colocalisation and study index
        interactions (DataFrame): Gene-gene interaction dataset with targetA, targetB, sourceDatabase, scoring columns
        target_index (TargetIndex): Target index with gene genomic locations
        string_threshold (float): Minimum STRING score to keep an interaction. Defaults to 0.75.
        intact_threshold (float): Minimum IntAct score to keep an interaction. Defaults to 0.42.
        delta (int): Maximum distance in bp between targetB TSS and the GWAS signal position. Defaults to 500_000.

    Returns:
        DataFrame: Feature annotation in long format with the columns: studyLocusId, geneId, featureName, featureValue
    """
    joining_cols = (
        ["studyLocusId", "geneId"]
        if isinstance(study_loci_to_annotate, L2GGoldStandard)
        else ["studyLocusId"]
    )

    coloc_methods = {c.lower() for c in ColocalisationMethod.get_method_names_for_metric(colocalisation_metric)}

    # Step 1: Trans-pQTL study loci and their measured genes (via study index)
    trans_pqtl_study_loci = (
        study_locus.filter(
            (f.col("isTransQtl").isNotNull()) & f.col("isTransQtl")
        )
        .df.select("studyLocusId", "studyId")
        .join(
            f.broadcast(study_index.df.filter(f.col("studyType") == "pqtl").select("studyId", "geneId")),
            on="studyId",
            how="inner",
        )
        .select(
            f.col("studyLocusId").alias("rightStudyLocusId"),
            f.col("geneId").alias("transPQTLGeneId"),
        )
        .filter(f.col("transPQTLGeneId").isNotNull())
    )

    # Step 2: Colocalisations where right side is a pQTL trans-locus, enriched with GWAS locus position.
    # Pre-filter to rightStudyType==pqtl and the relevant colocalisation methods before the joins
    # to minimise shuffle overhead.
    coloc_trans = (
        colocalisation.df.filter(
            (f.col("rightStudyType") == "pqtl")
            & f.lower("colocalisationMethod").isin(coloc_methods)
        )
        .select("leftStudyLocusId", "rightStudyLocusId", colocalisation_metric, "colocalisationMethod")
        .join(trans_pqtl_study_loci, on="rightStudyLocusId", how="inner")
        .join(
            study_locus.df.select("studyLocusId", "chromosome", "position").withColumnRenamed(
                "studyLocusId", "leftStudyLocusId"
            ),
            on="leftStudyLocusId",
            how="inner",
        )
    )

    # Step 3: Symmetric interactions enriched with targetB genomic location.
    # STRING and IntAct are kept above their respective score thresholds;
    # all other sources (Signor, Reactome, …) have no score requirement.
    gene_locations = target_index.locations_lut().select(
        f.col("geneId").alias("targetB"),
        f.col("chromosome").alias("targetB_chromosome"),
        f.col("tss").alias("targetB_tss"),
    )

    filtered_inter = (
        interactions.filter(
            ((f.col("sourceDatabase") == "string") & (f.col("scoring") >= string_threshold))
            | ((f.col("sourceDatabase") == "intact") & (f.col("scoring") >= intact_threshold))
            | (~f.col("sourceDatabase").isin("string", "intact"))
        )
        .select("targetA", "targetB")
        .distinct()
    )

    extended_inter = (
        filtered_inter.unionByName(
            filtered_inter.select(
                f.col("targetB").alias("targetA"),
                f.col("targetA").alias("targetB"),
            )
        )
        .filter(f.col("targetA") != f.col("targetB"))
        .filter(f.col("targetA").isNotNull())
        .filter(f.col("targetB").isNotNull())
        .distinct()
        .join(gene_locations, on="targetB", how="inner")
    )

    # Step 4: Join trans-pQTL colocalisations with interactions and apply geographic filter
    coloc_trans_inter = (
        coloc_trans.join(
            extended_inter.withColumnRenamed("targetA", "transPQTLGeneId"),
            on="transPQTLGeneId",
            how="inner",
        )
        .filter(f.col("targetB_chromosome") == f.col("chromosome"))
        .filter(f.abs(f.col("targetB_tss") - f.col("position")) <= delta)
    )

    # Step 5: Aggregate max colocalisation score per (GWAS locus, local gene)
    trans_interaction_feature = (
        coloc_trans_inter.select(
            f.col("leftStudyLocusId").alias("studyLocusId"),
            f.col("targetB").alias("geneId"),
            f.col(colocalisation_metric),
        )
        .groupBy("studyLocusId", "geneId")
        .agg(f.max(colocalisation_metric).alias(feature_name))
    )

    return (
        study_loci_to_annotate.df.join(
            trans_interaction_feature,
            on=joining_cols,
            how="inner",
        )
        .selectExpr("studyLocusId", "geneId", feature_name)
        .distinct()
    )


def common_neighbourhood_trans_pqtl_colocalisation_feature_logic(
    study_loci_to_annotate: StudyLocus | L2GGoldStandard,
    colocalisation_method: str,
    colocalisation_metric: str,
    feature_name: str,
    *,
    colocalisation: Colocalisation,
    study_index: StudyIndex,
    study_locus: StudyLocus,
    interactions: DataFrame,
    target_index: TargetIndex,
    variant_index: VariantIndex,
    string_threshold: float = 0.75,
    intact_threshold: float = 0.42,
    delta: int = 500_000,
) -> DataFrame:
    """Neighbourhood wrapper for trans-pQTL colocalisation features.

    Computes the base trans-pQTL colocalisation score per (studyLocus, gene), extends missing
    neighbourhood genes to 0, then normalises each gene's score by the regional maximum across
    all protein-coding genes at the locus.

    Args:
        study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
        colocalisation_method (str): The colocalisation method to filter the data by
        colocalisation_metric (str): The colocalisation metric to use
        feature_name (str): The name of the neighbourhood feature (must end with "Neighbourhood")
        colocalisation (Colocalisation): Dataset with the colocalisation results
        study_index (StudyIndex): Study index to fetch study type and gene
        study_locus (StudyLocus): Study locus to traverse between colocalisation and study index
        interactions (DataFrame): Gene-gene interaction dataset with targetA, targetB, sourceDatabase, scoring columns
        target_index (TargetIndex): Target index with gene genomic locations
        variant_index (VariantIndex): Variant index to annotate all overlapping neighbourhood genes
        string_threshold (float): Minimum STRING score to keep an interaction. Defaults to 0.75.
        intact_threshold (float): Minimum IntAct score to keep an interaction. Defaults to 0.42.
        delta (int): Maximum distance in bp between targetB TSS and the GWAS signal position. Defaults to 500_000.

    Returns:
        DataFrame: Feature annotation with columns studyLocusId, geneId, featureName containing normalised scores
    """
    local_feature_name = feature_name.replace("Neighbourhood", "")
    local_max = common_trans_pqtl_colocalisation_feature_logic(
        study_loci_to_annotate,
        colocalisation_method,
        colocalisation_metric,
        local_feature_name,
        colocalisation=colocalisation,
        study_index=study_index,
        study_locus=study_locus,
        interactions=interactions,
        target_index=target_index,
        string_threshold=string_threshold,
        intact_threshold=intact_threshold,
        delta=delta,
    )
    extended_local_max = local_max.unionByName(
        extend_missing_colocalisation_to_neighbourhood_genes(
            local_feature_name,
            local_max,
            variant_index,
            target_index,
            study_locus,
        )
    )
    return (
        extended_local_max.join(
            target_index.df.filter(f.col("biotype") == "protein_coding").select(
                f.col("id").alias("geneId")
            ),
            "geneId",
            "inner",
        )
        .withColumn(
            "regional_max",
            f.max(local_feature_name).over(Window.partitionBy("studyLocusId")),
        )
        .withColumn(
            feature_name,
            f.when(
                (f.col("regional_max").isNotNull()) & (f.col("regional_max") != 0.0),
                f.col(local_feature_name) / f.col("regional_max"),
            ).otherwise(f.lit(0.0)),
        )
        .drop("regional_max", local_feature_name)
    )


class TransPQtlColocH4MaximumFeature(L2GFeature):
    """Max H4 for each (study, locus, gene) aggregating over all trans-pQTLs."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus, SparkDataFrame, TargetIndex]
    feature_name = "transPQtlColocH4Maximum"
    string_threshold: float = 0.75
    intact_threshold: float = 0.42
    delta: int = 500_000

    @classmethod
    def compute(
        cls: type[TransPQtlColocH4MaximumFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> TransPQtlColocH4MaximumFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            TransPQtlColocH4MaximumFeature: Feature dataset
        """
        if "interactions" not in feature_dependency:
            raise ValueError(
                "Interactions dataframe is required for TransPQtlColocH4MaximumFeature."
            )
        if "target_index" not in feature_dependency:
            raise ValueError(
                "target_index is required for TransPQtlColocH4MaximumFeature."
            )

        colocalisation_method = "Coloc"
        colocalisation_metric = "h4"

        return cls(
            _df=convert_from_wide_to_long(
                common_trans_pqtl_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    string_threshold=cls.string_threshold,
                    intact_threshold=cls.intact_threshold,
                    delta=cls.delta,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )


class TransPQtlColocH4MaximumNeighbourhoodFeature(L2GFeature):
    """Max H4 for each (study, locus) aggregating over all trans-pQTLs, normalised by the regional maximum."""

    feature_dependency_type = [Colocalisation, StudyIndex, StudyLocus, SparkDataFrame, TargetIndex, VariantIndex]
    feature_name = "transPQtlColocH4MaximumNeighbourhood"
    string_threshold: float = 0.75
    intact_threshold: float = 0.42
    delta: int = 500_000

    @classmethod
    def compute(
        cls: type[TransPQtlColocH4MaximumNeighbourhoodFeature],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        feature_dependency: dict[str, Any],
    ) -> TransPQtlColocH4MaximumNeighbourhoodFeature:
        """Computes the feature.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): The dataset containing study loci that will be used for annotation
            feature_dependency (dict[str, Any]): Dataset with the colocalisation results

        Returns:
            TransPQtlColocH4MaximumNeighbourhoodFeature: Feature dataset
        """
        if "interactions" not in feature_dependency:
            raise ValueError(
                "Interactions dataframe is required for TransPQtlColocH4MaximumNeighbourhoodFeature."
            )
        if "target_index" not in feature_dependency:
            raise ValueError(
                "target_index is required for TransPQtlColocH4MaximumNeighbourhoodFeature."
            )

        colocalisation_method = "Coloc"
        colocalisation_metric = "h4"

        return cls(
            _df=convert_from_wide_to_long(
                common_neighbourhood_trans_pqtl_colocalisation_feature_logic(
                    study_loci_to_annotate,
                    colocalisation_method,
                    colocalisation_metric,
                    cls.feature_name,
                    string_threshold=cls.string_threshold,
                    intact_threshold=cls.intact_threshold,
                    delta=cls.delta,
                    **feature_dependency,
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=cls.get_schema(),
        )
