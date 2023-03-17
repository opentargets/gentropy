"""Unit test configuration."""
from __future__ import annotations

import dbldatagen as dg
import pytest
from pyspark.sql import DataFrame, SparkSession

from otg.common.schemas import parse_spark_schema
from otg.dataset.colocalisation import Colocalisation
from otg.dataset.study_index import StudyIndex, StudyIndexGWASCatalog
from otg.dataset.study_locus import StudyLocus, StudyLocusGWASCatalog
from otg.dataset.study_locus_overlap import StudyLocusOverlap
from otg.dataset.v2g import V2G
from otg.dataset.variant_index import VariantIndex


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Local spark session for testing purposes.

    Returns:
        SparkSession: local spark session
    """
    return (
        SparkSession.builder.config("spark.driver.bindAddress", "127.0.0.1")
        .master("local")
        .appName("test")
        .getOrCreate()
    )


@pytest.fixture()
def mock_colocalisation(spark: SparkSession) -> Colocalisation:
    """Mock colocalisation dataset."""
    schema = parse_spark_schema("colocalisation.json")

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(schema)
        .withColumnSpec("coloc_h0", percentNulls=0.1)
        .withColumnSpec("coloc_h1", percentNulls=0.1)
        .withColumnSpec("coloc_h2", percentNulls=0.1)
        .withColumnSpec("coloc_h3", percentNulls=0.1)
        .withColumnSpec("coloc_h4", percentNulls=0.1)
        .withColumnSpec("coloc_log2_h4_h3", percentNulls=0.1)
        .withColumnSpec("clpp", percentNulls=0.1)
    )

    return Colocalisation(_df=data_spec.build(), _schema=schema)


def mock_study_index_data(spark: SparkSession) -> DataFrame:
    """Mock v2g dataset."""
    schema = parse_spark_schema("studies.json")

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(schema)
        .withColumnSpec(
            "traitFromSourceMappedIds",
            expr="array(cast(rand() AS string))",
            percentNulls=0.1,
        )
        .withColumnSpec(
            "backgroundTraitFromSourceMappedIds",
            expr="array(cast(rand() AS string))",
            percentNulls=0.1,
        )
        .withColumnSpec(
            "discoverySamples",
            expr='array(named_struct("sampleSize", cast(rand() as string), "ancestry", cast(rand() as string)))',
            percentNulls=0.1,
        )
        .withColumnSpec(
            "replicationSamples",
            expr='array(named_struct("sampleSize", cast(rand() as string), "ancestry", cast(rand() as string)))',
            percentNulls=0.1,
        )
        .withColumnSpec("pubmedId", percentNulls=0.1)
        .withColumnSpec("publicationFirstAuthor", percentNulls=0.1)
        .withColumnSpec("publicationDate", percentNulls=0.1)
        .withColumnSpec("publicationJournal", percentNulls=0.1)
        .withColumnSpec("publicationTitle", percentNulls=0.1)
        .withColumnSpec("initialSampleSize", percentNulls=0.1)
        .withColumnSpec("nCases", percentNulls=0.1)
        .withColumnSpec("nControls", percentNulls=0.1)
        .withColumnSpec("nSamples", percentNulls=0.1)
        .withColumnSpec("summarystatsLocation", percentNulls=0.1)
    )
    return data_spec.build()


@pytest.fixture()
def mock_study_index(spark: SparkSession) -> StudyIndex:
    """Mock StudyIndex dataset."""
    return StudyIndex(
        _df=mock_study_index_data(spark),
        _schema=parse_spark_schema("studies.json"),
    )


@pytest.fixture()
def mock_study_index_gwas_catalog(spark: SparkSession) -> StudyIndexGWASCatalog:
    """Mock StudyIndexGWASCatalog dataset."""
    return StudyIndexGWASCatalog(
        _df=mock_study_index_data(spark),
        _schema=parse_spark_schema("studies.json"),
    )


@pytest.fixture()
def mock_study_locus_overlap(spark: SparkSession) -> StudyLocusOverlap:
    """Mock StudyLocusOverlap dataset."""
    schema = parse_spark_schema("study_locus_overlap.json")

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(schema)
        .withColumnSpec("right_logABF", percentNulls=0.1)
        .withColumnSpec("left_logABF", percentNulls=0.1)
        .withColumnSpec("right_posteriorProbability", percentNulls=0.1)
        .withColumnSpec("left_posteriorProbability", percentNulls=0.1)
    )

    return StudyLocusOverlap(_df=data_spec.build(), _schema=schema)


def mock_study_locus_data(spark: SparkSession) -> DataFrame:
    """Mock study_locus dataset."""
    schema = parse_spark_schema("study_locus.json")

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(schema)
        .withColumnSpec("chromosome", percentNulls=0.1)
        .withColumnSpec("position", percentNulls=0.1)
        .withColumnSpec("beta", percentNulls=0.1)
        .withColumnSpec("oddsRatio", percentNulls=0.1)
        .withColumnSpec("oddsRatioConfidenceIntervalLower", percentNulls=0.1)
        .withColumnSpec("oddsRatioConfidenceIntervalUpper", percentNulls=0.1)
        .withColumnSpec("betaConfidenceIntervalLower", percentNulls=0.1)
        .withColumnSpec("betaConfidenceIntervalUpper", percentNulls=0.1)
        .withColumnSpec("subStudyDescription", percentNulls=0.1)
        .withColumnSpec("pValueMantissa", percentNulls=0.1)
        .withColumnSpec("pValueExponent", percentNulls=0.1)
        .withColumnSpec(
            "qualityControls",
            expr="array(cast(rand() as string))",
            percentNulls=0.1,
        )
        .withColumnSpec("finemappingMethod", percentNulls=0.1)
        .withColumnSpec(
            "credibleSet",
            expr='array(named_struct("is95CredibleSet", cast(rand() > 0.5 as boolean), "is99CredibleSet", cast(rand() > 0.5 as boolean), "logABF", rand(), "posteriorProbability", rand(), "tagVariantId", cast(rand() as string), "tagPValue", rand(), "tagPValueConditioned", rand(), "tagBeta", rand(), "tagStandardError", rand(), "tagBetaConditioned", rand(), "tagStandardErrorConditioned", rand(), "r2Overall", rand()))',
            percentNulls=0.1,
        )
    )
    return data_spec.build()


@pytest.fixture()
def mock_study_locus(spark: SparkSession) -> StudyLocus:
    """Mock study_locus dataset."""
    return StudyLocus(
        _df=mock_study_locus_data(spark),
        _schema=parse_spark_schema("study_locus.json"),
    )


@pytest.fixture()
def mock_study_locus_gwas_catalog(spark: SparkSession) -> StudyLocus:
    """Mock study_locus dataset."""
    return StudyLocusGWASCatalog(
        _df=mock_study_locus_data(spark),
        _schema=parse_spark_schema("study_locus.json"),
    )


@pytest.fixture()
def mock_v2g(spark: SparkSession) -> V2G:
    """Mock v2g dataset."""
    v2g_schema = parse_spark_schema("v2g.json")

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(v2g_schema)
        .withColumnSpec("distance", percentNulls=0.1)
        .withColumnSpec("resourceScore", percentNulls=0.1)
        .withColumnSpec("pmid", percentNulls=0.1)
        .withColumnSpec("biofeature", percentNulls=0.1)
        .withColumnSpec("score", percentNulls=0.1)
        .withColumnSpec("label", percentNulls=0.1)
        .withColumnSpec("variantFunctionalConsequenceId", percentNulls=0.1)
        .withColumnSpec("isHighQualityPlof", percentNulls=0.1)
    )

    return V2G(_df=data_spec.build(), _schema=v2g_schema)


@pytest.fixture()
def mock_variant_index(spark: SparkSession) -> VariantIndex:
    """Mock gene index."""
    vi_schema = parse_spark_schema("variant_index.json")

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
        )
        .withSchema(vi_schema)
        .withColumnSpec("chromosomeB37", percentNulls=0.1)
        .withColumnSpec("positionB37", percentNulls=0.1)
        .withColumnSpec("mostSevereConsequence", percentNulls=0.1)
        # Nested column handling workaround
        # https://github.com/databrickslabs/dbldatagen/issues/135
        # It's a workaround for nested column handling in dbldatagen.
        .withColumnSpec(
            "alleleFrequencies",
            expr='array(named_struct("alleleFrequency", rand(), "populationName", cast(rand() as string)))',
            percentNulls=0.1,
        )
        .withColumnSpec(
            "cadd",
            expr='named_struct("phred", cast(rand() AS float), "raw", cast(rand() AS float))',
            percentNulls=0.1,
        )
        .withColumnSpec(
            "filters", expr="array(cast(rand() AS string))", percentNulls=0.1
        )
        .withColumnSpec("rsIds", expr="array(cast(rand() AS string))", percentNulls=0.1)
    )
    data_spec.build().printSchema()

    return VariantIndex(_df=data_spec.build(), _schema=vi_schema)
