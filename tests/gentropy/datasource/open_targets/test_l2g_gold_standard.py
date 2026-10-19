"""Test Open Targets L2G gold standards data source."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from gentropy.dataset.credible_set import CredibleSet, CredibleSetLocus
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.locus_overlap import LocusOverlap
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.open_targets.l2g_gold_standard import (
    OpenTargetsL2GGoldStandard,
)
from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader

if TYPE_CHECKING:
    from pyspark.sql.session import SparkSession

    from gentropy.dataset.colocalisation import Colocalisation


def _credible_sets_led_by_curation(curation: DataFrame) -> CredibleSet:
    """One credible set per curated study and sentinel, keyed `cs_<study>_<sentinel>`.

    Args:
        curation (DataFrame): OTG gold standard curation.

    Returns:
        CredibleSet: Credible sets led by the curated sentinels.
    """
    leads = curation.select(
        f.col("association_info.otg_id").alias("studyId"),
        f.concat_ws(
            "_",
            f.col("sentinel_variant.locus_GRCh38.chromosome"),
            f.col("sentinel_variant.locus_GRCh38.position"),
            f.col("sentinel_variant.alleles.reference"),
            f.col("sentinel_variant.alleles.alternative"),
        ).alias("leadVariantId"),
    ).distinct()
    schema = CredibleSet.get_schema()
    constants = {
        "id": f.concat_ws("_", f.lit("cs"), "studyId", "leadVariantId"),
        "chromosome": f.lit("1"),
        "locusStart": f.lit(1),
        "locusEnd": f.lit(1),
        "studyType": f.lit("gwas"),
        "fineMappingMethod": f.lit("SuSiE"),
        "qualityControls": f.array(),
    }
    return CredibleSet(
        _df=leads.select(
            *(
                f.col(field.name)
                if field.name in leads.columns
                else constants.get(field.name, f.lit(None))
                .cast(field.dataType)
                .alias(field.name)
                for field in schema.fields
            )
        ),
        _schema=schema,
    )


def test_open_targets_as_l2g_gold_standard(
    sample_l2g_gold_standard: DataFrame,
    mock_variant_index: VariantIndex,
) -> None:
    """Test L2G gold standard from OTG curation."""
    assert isinstance(
        OpenTargetsL2GGoldStandard.as_l2g_gold_standard(
            sample_l2g_gold_standard,
            _credible_sets_led_by_curation(sample_l2g_gold_standard),
            mock_variant_index,
        ),
        L2GGoldStandard,
    )


def test_parse_positive_curation(
    sample_l2g_gold_standard: DataFrame,
) -> None:
    """Curated positives take the id of the credible set their sentinel leads."""
    expected_cols = ["credibleSetId", "studyId", "variantId", "geneId", "sources"]
    df = OpenTargetsL2GGoldStandard.parse_positive_curation(
        sample_l2g_gold_standard,
        _credible_sets_led_by_curation(sample_l2g_gold_standard),
    )
    assert df.columns == expected_cols, "GS parsing has a different schema."
    assert df.count() > 0
    assert (
        df.filter(
            f.col("credibleSetId")
            != f.concat_ws("_", f.lit("cs"), "studyId", "variantId")
        ).count()
        == 0
    )


def test_parse_positive_curation_drops_sentinels_leading_no_credible_set(
    sample_l2g_gold_standard: DataFrame,
) -> None:
    """Curated associations without a credible set led by their sentinel are dropped."""
    credible_sets = _credible_sets_led_by_curation(sample_l2g_gold_standard)
    df = OpenTargetsL2GGoldStandard.parse_positive_curation(
        sample_l2g_gold_standard,
        CredibleSet(_df=credible_sets.df.limit(0), _schema=CredibleSet.get_schema()),
    )
    assert df.count() == 0


class TestExpandGoldStandardWithNegatives:
    """Test expanding positive set with negative set."""

    observed_df: DataFrame
    expected_expanded_gs: DataFrame
    sample_positive_set: DataFrame

    def test_expand_gold_standard_with_negatives_logic(
        self: TestExpandGoldStandardWithNegatives, spark: SparkSession
    ) -> None:
        """Test expanding positive set with negative set coincides with expected results."""
        assert self.observed_df.collect() == self.expected_expanded_gs.collect(), (
            "GS expansion is not as expected."
        )

    def test_expand_gold_standard_with_negatives_same_positives(
        self: TestExpandGoldStandardWithNegatives, spark: SparkSession
    ) -> None:
        """Test expanding positive set with negative set doesn't remove any positives."""
        assert (
            self.observed_df.filter("goldStandardSet == 'positive'").count()
            == self.sample_positive_set.count()
        ), "GS expansion has removed positives."

    @pytest.fixture(autouse=True)
    def _setup(self: TestExpandGoldStandardWithNegatives, spark: SparkSession) -> None:
        """Prepare fixtures for TestExpandGoldStandardWithNegatives."""
        self.sample_positive_set = spark.createDataFrame(
            [
                ("variant1", "gene1", "study1"),
                ("variant2", "gene2", "study1"),
            ],
            ["variantId", "geneId", "studyId"],
        )

        sample_variant_index_df = spark.createDataFrame(
            [
                (
                    "variant1",
                    "chrom",
                    1,
                    "A",
                    "T",
                    [
                        {
                            "distanceFromTss": 5,
                            "targetId": "gene1",
                            "isEnsemblCanonical": True,
                        },
                        {
                            "distanceFromTss": 10,
                            "targetId": "gene3",
                            "isEnsemblCanonical": True,
                        },
                    ],
                ),
            ],
            StructType(
                [
                    StructField("variantId", StringType(), True),
                    StructField("chromosome", StringType(), True),
                    StructField("position", IntegerType(), True),
                    StructField("referenceAllele", StringType(), True),
                    StructField("alternateAllele", StringType(), True),
                    StructField(
                        "transcriptConsequences",
                        ArrayType(
                            StructType(
                                [
                                    StructField("distanceFromTss", LongType(), True),
                                    StructField("targetId", StringType(), True),
                                    StructField(
                                        "isEnsemblCanonical", BooleanType(), True
                                    ),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            ),
        )

        self.expected_expanded_gs = spark.createDataFrame(
            [
                ("variant1", "study1", "negative", "gene3"),
                ("variant1", "study1", "positive", "gene1"),
                ("variant2", "study1", "positive", "gene2"),
            ],
            ["variantId", "geneId", "goldStandardSet", "studyId"],
        )
        self.observed_df = (
            OpenTargetsL2GGoldStandard.expand_gold_standard_with_negatives(
                self.sample_positive_set,
                VariantIndex(
                    _df=sample_variant_index_df, _schema=VariantIndex.get_schema()
                ),
            )
        )


def test_build_feature_matrix(
    mock_l2g_gold_standard: L2GGoldStandard,
    mock_credible_set_locus: CredibleSetLocus,
    mock_credible_set: CredibleSet,
    mock_colocalisation: Colocalisation,
    mock_study_index: StudyIndex,
    mock_variant_index: VariantIndex,
) -> None:
    """Test building feature matrix with the eQtlColocH4Maximum and isProteinCoding features."""
    features_list = ["eQtlColocH4Maximum", "isProteinCoding"]
    loader = L2GFeatureInputLoader(
        colocalisation=mock_colocalisation,
        study_index=mock_study_index,
        study_locus=mock_credible_set_locus,
        variant_index=mock_variant_index,
    )
    fm = mock_credible_set_locus.build_feature_matrix(features_list, loader)
    assert isinstance(
        mock_l2g_gold_standard.build_feature_matrix(fm, mock_credible_set),
        L2GFeatureMatrix,
    ), "Feature matrix should be of type L2GFeatureMatrix"


def test_filter_unique_associations_drops_overlapping_locus_of_same_gene(
    spark: SparkSession,
) -> None:
    """Curated credible sets pointing to one gene and sharing a variant are no longer both kept."""
    gold_standard = L2GGoldStandard(
        _df=spark.createDataFrame(
            [
                ("csA", "1_10_A_T", "S1", "gene1", None, "positive", ["source"]),
                ("csB", "1_20_C_G", "S2", "gene1", None, "positive", ["source"]),
            ],
            L2GGoldStandard.get_schema(),
        ),
        _schema=L2GGoldStandard.get_schema(),
    )
    overlaps = LocusOverlap(
        _df=spark.createDataFrame(
            [("csA", "csB", "gwas", "1", "1_10_A_T", None)],
            LocusOverlap.get_schema(),
        ),
        _schema=LocusOverlap.get_schema(),
    )
    kept = gold_standard.filter_unique_associations(overlaps)
    assert [row.credibleSetId for row in kept.df.collect()] == ["csB"]
