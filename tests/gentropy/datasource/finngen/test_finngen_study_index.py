"""Tests for study index dataset from FinnGen."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import types as t

from gentropy.dataset.study_index import StudyIndex
from gentropy.datasource.finngen.study_index import FinnGenStudyIndex


def test_finngen_study_index_from_source(spark: SparkSession) -> None:
    """Test study index from source."""
    assert isinstance(FinnGenStudyIndex.from_source(spark), StudyIndex)


def test_finngen_study_index_add_efos(spark: SparkSession) -> None:
    """Test finngen study index add efo ids."""
    study_index_table_data = [
        (
            "AB1_1",
            "Actinomycosis",
            "FINNGEN_R11",
            "gwas",
        ),
        (
            "AB1_2",
            "Some unknown trait",
            "FINNGEN_R11",
            "gwas",
        ),
        (
            "AB1_1",
            "Some unknown trait",
            "FINNGEN_R11",
            "gwas",
        ),
        (
            "AB1_1",
            "Bleeding",
            "FINNGEN_R11",
            "gwas",
        ),
    ]
    study_index_df = spark.createDataFrame(
        data=study_index_table_data,
        schema=t.StructType(
            [
                t.StructField("studyId", t.StringType(), nullable=False),
                t.StructField("traitFromSource", t.StringType(), nullable=False),
                t.StructField("projectId", t.StringType(), nullable=False),
                t.StructField("studyType", t.StringType(), nullable=False),
            ]
        ),
    )

    curation_table_data = [
        ("FinnGen r11", "Actinomycosis", "http://www.ebi.ac.uk/efo/EFO_0007128"),
        ("FinnGen r11", "bleeding", "http://purl.obolibrary.org/obo/MP_0001914"),
        ("FinnGen r11", "Bruxism", "http://purl.obolibrary.org/obo/MONDO_0002443"),
        (
            "PheWAS 2024",
            "20161#Pack years of smoking",
            "http://www.ebi.ac.uk/efo/EFO_0005671",
        ),
    ]
    curation_df = spark.createDataFrame(
        data=curation_table_data,
        schema=t.StructType(
            [
                t.StructField("STUDY", t.StringType(), nullable=False),
                t.StructField("PROPERTY_VALUE", t.StringType(), nullable=False),
                t.StructField("SEMANTIC_TAG", t.StringType(), nullable=False),
            ]
        ),
    )

    study_index = StudyIndex(_df=study_index_df, _schema=study_index_df.schema)
    assert isinstance(
        FinnGenStudyIndex.join_efo_mapping(
            study_index,
            finngen_release_prefix="FINNGEN_R11_",
            efo_curation_mapping=curation_df,
        ),
        StudyIndex,
    )
