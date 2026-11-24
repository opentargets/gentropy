"""Random datasets for the clumping tests."""

from __future__ import annotations

import dbldatagen as dg
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as t

from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import AnnotatedStudyLocus


def _mandatory(schema: t.StructType, *optional: str) -> t.StructType:
    """Keep the mandatory columns of a dataset schema, plus the named optional ones.

    Args:
        schema (t.StructType): The dataset schema.
        *optional (str): Optional columns to keep as well.

    Returns:
        t.StructType: The kept columns, in schema order.
    """
    return t.StructType(
        [field for field in schema if not field.nullable or field.name in optional]
    )


@pytest.fixture()
def random_ld_index(spark: SparkSession) -> LDIndex:
    """Random LD index on chromosome 1, keyed by the row number as variant id.

    Args:
        spark (SparkSession): Spark session.

    Returns:
        LDIndex: 400 panel variants, each tagging one random variant.
    """
    data_spec = (
        dg.DataGenerator(
            spark, rows=400, partitions=4, randomSeedMethod="hash_fieldname"
        )
        .withSchema(LDIndex.get_schema())
        .withColumnSpec("variantId", expr="cast(id as string)")
        .withColumnSpec("chromosome", expr="'1'")
        .withColumnSpec(
            "ldSet",
            expr="array(named_struct('tagVariantId', cast(rand() as string), "
            "'rValues', array(named_struct('population', cast(rand() as string), "
            "'r', cast(rand() as double)))))",
        )
    )
    return LDIndex(_df=data_spec.build(), _schema=LDIndex.get_schema())


@pytest.fixture()
def random_study_index(spark: SparkSession) -> StudyIndex:
    """Random study index with its mandatory columns, keyed by the row number.

    Args:
        spark (SparkSession): Spark session.

    Returns:
        StudyIndex: 400 studies, a tenth without population structure.
    """
    data_spec = (
        dg.DataGenerator(
            spark, rows=400, partitions=4, randomSeedMethod="hash_fieldname"
        )
        .withSchema(
            _mandatory(
                StudyIndex.get_schema(), "ldPopulationStructure", "qualityControls"
            )
        )
        .withColumnSpec("studyId", expr="cast(id as string)")
        .withColumnSpec(
            "ldPopulationStructure",
            expr="array(named_struct('ldPopulation', cast(rand() as string), "
            "'relativeSampleSize', rand()))",
            percentNulls=0.1,
        )
        .withColumnSpec(
            "qualityControls", expr="array(cast(rand() as string))", percentNulls=0.1
        )
    )
    return StudyIndex(_df=data_spec.build(), _schema=StudyIndex.get_schema())


@pytest.fixture()
def random_annotated_study_locus(spark: SparkSession) -> AnnotatedStudyLocus:
    """Random annotated study loci on chromosome 1, one per study of the random index.

    Args:
        spark (SparkSession): Spark session.

    Returns:
        AnnotatedStudyLocus: 400 loci whose lead is the LD index variant of the
            same row number.
    """
    # The seed column is renamed, as ``id`` is the study locus key.
    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
            seedColumnName="row",
        )
        .withSchema(_mandatory(AnnotatedStudyLocus.get_schema()))
        .withColumnSpec("id", expr="cast(row as string)")
        .withColumnSpec("studyId", expr="cast(row as string)")
        .withColumnSpec("chromosome", expr="'1'")
        .withColumnSpec("leadVariantId", expr="cast(row as string)", percentNulls=0.1)
        .withColumnSpec(
            "qualityControls", expr="array(cast(rand() as string))", percentNulls=0.1
        )
        .withColumnSpec(
            "locus",
            expr="array(named_struct('variantId', cast(row as string), "
            "'pValueMantissa', cast(rand() as float), 'pValueExponent', -8, "
            "'effectAlleleFrequencyFromSource', cast(rand() as float), "
            "'sampleSize', 1000, 'beta', rand(), 'standardError', rand()))",
        )
    )
    return AnnotatedStudyLocus(
        _df=data_spec.build(), _schema=AnnotatedStudyLocus.get_schema()
    )
