"""Therapeutic area dataset."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from pyspark.sql import functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark import string2camelcase
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


class TraitClassName(StrEnum):
    """Trait class names used for binary/quantitative classification.

    Attributes:
        BINARY: Study with case and control samples (e.g. disease GWAS).
        QUANTITATIVE: Study measuring a continuous trait (e.g. QTL or measurement GWAS).
        UNKNOWN: Trait class could not be determined.
    """

    BINARY = "binary"
    QUANTITATIVE = "quantitative"
    UNKNOWN = "misclassified_phenotype"


class TherapeuticAreaHierarchy(StrEnum):
    """Therapeutic areas hierarchy.

    The order of entries defines priority when multiple therapeutic areas match a disease's
    ancestors — the first matching entry in the enum wins.

    The special entry EFO_0001444 ("measurement") is used to classify GWAS studies as
    quantitative rather than binary.
    """

    EFO_0001444 = "measurement"
    MONDO_0045024 = "cancer or benign tumor"
    OTAR_0000018 = "genetic familial or congenital disease"
    EFO_0005741 = "infectious disease"
    OTAR_0000009 = "injury poisoning or other complication"
    OTAR_0000014 = "pregnancy or perinatal disease"
    MONDO_0024458 = "disorder of visual system"
    EFO_0000319 = "cardiovascular disease"
    EFO_0009605 = "pancreas disease"
    EFO_0010282 = "gastrointestinal disease"
    OTAR_0000017 = "reproductive system or breast disease"
    EFO_0010285 = "integumentary system disease"
    EFO_0001379 = "endocrine system disease"
    OTAR_0000010 = "respiratory or thoracic disease"
    EFO_0009690 = "urinary system disease"
    OTAR_0000006 = "musculoskeletal or connective tissue disease"
    MONDO_0021205 = "disorder of ear"
    EFO_0000540 = "immune system disease"
    EFO_0005803 = "hematologic disease"
    EFO_0000618 = "nervous system disease"
    MONDO_0002025 = "psychiatric disorder"
    OTAR_0000020 = "nutritional or metabolic disease"
    EFO_0003765 = "sign or symptom"


@dataclass
class TherapeuticArea(Dataset):
    """Therapeutic area dataset.

    A flat many-to-many mapping from disease identifiers to therapeutic area identifiers,
    derived from the Open Targets disease dataset. The schema is intentionally minimal:
    one row per (diseaseId, therapeuticAreaId) pair.

    This dataset is the basis for classifying GWAS studies as binary or quantitative via
    the therapeutic area hierarchy: diseases whose primary therapeutic area is
    EFO_0001444 ("measurement") are quantitative; all others are binary.
    """

    @classmethod
    def get_schema(cls: type[TherapeuticArea]) -> StructType:
        """Provide the schema for the TherapeuticArea dataset.

        Returns:
            StructType: The schema of the TherapeuticArea dataset.
        """
        return parse_spark_schema("therapeutic_area.json")

    @classmethod
    def from_disease(cls: type[TherapeuticArea], disease: DataFrame) -> TherapeuticArea:
        """Construct a TherapeuticArea dataset from the Open Targets disease DataFrame.

        The disease DataFrame is expected to contain an "id" column and a "therapeuticAreas"
        array column. Rows where therapeuticAreas is null or empty are dropped. The result
        is a flat (diseaseId, therapeuticAreaId) table with duplicates allowed, reflecting
        the many-to-many nature of the relationship.

        Args:
            disease (DataFrame): Open Targets disease DataFrame with columns "id" and
                "therapeuticAreas" (array of strings).

        Returns:
            TherapeuticArea: Dataset with one row per (diseaseId, therapeuticAreaId) pair.

        Examples:
            >>> from gentropy.dataset.therapeutic_area import TherapeuticArea
            >>> from pyspark.sql import functions as f
            >>> data = [
            ...     ("EFO_000001", ["EFO_0001444", "MONDO_0045024"]),
            ...     ("EFO_000002", ["EFO_0000319"]),
            ...     ("EFO_000003", []),
            ... ]
            >>> schema = "id STRING, therapeuticAreas ARRAY<STRING>"
            >>> disease_df = spark.createDataFrame(data, schema)
            >>> ta = TherapeuticArea.from_disease(disease_df)
            >>> ta.df.orderBy("diseaseId", "therapeuticAreaId").show(truncate=False)
            +----------+-----------------+
            |diseaseId |therapeuticAreaId|
            +----------+-----------------+
            |EFO_000001|EFO_0001444      |
            |EFO_000001|MONDO_0045024    |
            |EFO_000002|EFO_0000319      |
            +----------+-----------------+
            <BLANKLINE>
        """
        df = (
            disease.select(
                f.col("id").alias("diseaseId"),
                f.explode("therapeuticAreas").alias("therapeuticAreaId"),
            )
            .filter(f.col("therapeuticAreaId").isNotNull())
        )
        return cls(_df=df, _schema=cls.get_schema())

    def get_primary_therapeutic_area(
        self: TherapeuticArea,
        ta_hierarchy: type[TherapeuticAreaHierarchy] = TherapeuticAreaHierarchy,
    ) -> DataFrame:
        """Resolve the primary therapeutic area for each disease based on the hierarchy.

        For each disease, the therapeutic area with the lowest index in
        TherapeuticAreaHierarchy wins. Diseases with no matching therapeutic area in the
        hierarchy are assigned "other".

        Args:
            ta_hierarchy (type[TherapeuticAreaHierarchy]): Enum defining the priority order
                of therapeutic areas. Defaults to TherapeuticAreaHierarchy.

        Returns:
            DataFrame: with columns "diseaseId" and "primaryTherapeuticArea" (EFO id string).

        Examples:
            >>> from gentropy.dataset.therapeutic_area import TherapeuticArea
            >>> from pyspark.sql import functions as f
            >>> data = [
            ...     ("EFO_000001", ["EFO_0001444", "MONDO_0045024"]),
            ...     ("EFO_000002", ["MONDO_0045024"]),
            ...     ("EFO_000003", ["UNKNOWN_ID"]),
            ... ]
            >>> schema = "id STRING, therapeuticAreas ARRAY<STRING>"
            >>> disease_df = spark.createDataFrame(data, schema)
            >>> ta = TherapeuticArea.from_disease(disease_df)
            >>> ta.get_primary_therapeutic_area().orderBy("diseaseId").show(truncate=False)
            +----------+----------------------+
            |diseaseId |primaryTherapeuticArea|
            +----------+----------------------+
            |EFO_000001|EFO_0001444           |
            |EFO_000002|MONDO_0045024         |
            |EFO_000003|other                 |
            +----------+----------------------+
            <BLANKLINE>
        """
        # Build a priority-ordered struct array: [{name, index}, ...]
        hierarchy_index = f.array(
            *[
                f.struct(f.lit(ta.name).alias("name"), f.lit(idx).alias("index"))
                for idx, ta in enumerate(ta_hierarchy)
            ]
        )
        # Collect all therapeuticAreaIds per disease as an array, then find the
        # hierarchy entry with the lowest index that appears in the disease's areas.
        primary_ta = (
            self.df
            .groupBy("diseaseId")
            .agg(f.collect_set("therapeuticAreaId").alias("therapeuticAreaIds"))
            .withColumn(
                "primaryTherapeuticArea",
                f.filter(hierarchy_index, lambda x: f.array_contains(f.col("therapeuticAreaIds"), x.getField("name")))
                .getItem(0)
                .getField("name"),
            )
            .withColumn(
                "primaryTherapeuticArea",
                f.coalesce(f.col("primaryTherapeuticArea"), f.lit("other")),
            )
            .select("diseaseId", "primaryTherapeuticArea")
        )
        return primary_ta

    def classify_trait(
        self: TherapeuticArea,
        ta_hierarchy: type[TherapeuticAreaHierarchy] = TherapeuticAreaHierarchy,
        measurement_ta_id: str = TherapeuticAreaHierarchy.EFO_0001444.name,
    ) -> DataFrame:
        """Classify each disease as binary or quantitative based on its primary therapeutic area.

        A disease is classified as quantitative if its primary therapeutic area is the
        measurement therapeutic area (EFO_0001444 by default). All others are binary.
        Diseases with no match in the hierarchy ("other") are classified as misclassified_phenotype.

        Args:
            ta_hierarchy (type[TherapeuticAreaHierarchy]): Enum defining the priority order.
                Defaults to TherapeuticAreaHierarchy.
            measurement_ta_id (str): The therapeutic area ID that marks measurement/quantitative
                traits. Defaults to EFO_0001444.

        Returns:
            DataFrame: with columns "diseaseId" and "traitClass" (TraitClassName value).

        Examples:
            >>> from gentropy.dataset.therapeutic_area import TherapeuticArea, TraitClassName
            >>> from pyspark.sql import functions as f
            >>> data = [
            ...     ("EFO_000001", ["EFO_0001444"]),           # measurement -> quantitative
            ...     ("EFO_000002", ["MONDO_0045024"]),         # disease -> binary
            ...     ("EFO_000003", ["UNKNOWN_ID"]),            # no match -> misclassified_phenotype
            ... ]
            >>> schema = "id STRING, therapeuticAreas ARRAY<STRING>"
            >>> disease_df = spark.createDataFrame(data, schema)
            >>> ta = TherapeuticArea.from_disease(disease_df)
            >>> ta.classify_trait().orderBy("diseaseId").show(truncate=False)
            +----------+-----------------------+
            |diseaseId |traitClass             |
            +----------+-----------------------+
            |EFO_000001|quantitative           |
            |EFO_000002|binary                 |
            |EFO_000003|misclassified_phenotype|
            +----------+-----------------------+
            <BLANKLINE>
        """
        primary = self.get_primary_therapeutic_area(ta_hierarchy)
        return primary.withColumn(
            "traitClass",
            f.when(
                f.col("primaryTherapeuticArea") == f.lit(measurement_ta_id),
                f.lit(TraitClassName.QUANTITATIVE.value),
            )
            .when(
                f.col("primaryTherapeuticArea") == f.lit("other"),
                f.lit(TraitClassName.UNKNOWN.value),
            )
            .otherwise(f.lit(TraitClassName.BINARY.value)),
        ).select("diseaseId", "traitClass")

    def pivot_therapeutic_areas(
        self: TherapeuticArea,
        ta_hierarchy: type[TherapeuticAreaHierarchy] = TherapeuticAreaHierarchy,
    ) -> DataFrame:
        """Pivot therapeutic area assignments into a wide study-level boolean flag table.

        Each therapeutic area in the hierarchy becomes a camelCase boolean column indicating
        whether a disease belongs to that therapeutic area. The result is joined back to the
        full disease set so every disease has a row.

        Args:
            ta_hierarchy (type[TherapeuticAreaHierarchy]): Enum defining which therapeutic
                areas to pivot. Defaults to TherapeuticAreaHierarchy.

        Returns:
            DataFrame: with "diseaseId" and one boolean column per therapeutic area in the
                hierarchy, named in camelCase (e.g. "measurementCount", "cancerOrBenignTumorCount").

        Examples:
            >>> from gentropy.dataset.therapeutic_area import TherapeuticArea
            >>> from pyspark.sql import functions as f
            >>> data = [
            ...     ("EFO_000001", ["EFO_0001444", "MONDO_0045024"]),
            ...     ("EFO_000002", ["MONDO_0045024"]),
            ... ]
            >>> schema = "id STRING, therapeuticAreas ARRAY<STRING>"
            >>> disease_df = spark.createDataFrame(data, schema)
            >>> ta = TherapeuticArea.from_disease(disease_df)
            >>> pivot = ta.pivot_therapeutic_areas()
            >>> "measurement" in pivot.columns or any("measurement" in c.lower() for c in pivot.columns)
            True
        """
        known_ta_ids = {ta.name for ta in ta_hierarchy}
        # Filter to only hierarchy-known TAs before pivoting to avoid sparse garbage columns
        pivoted = (
            self.df
            .filter(f.col("therapeuticAreaId").isin(list(known_ta_ids)))
            .groupBy("diseaseId")
            .pivot("therapeuticAreaId", list(known_ta_ids))
            .count()
        )
        # Rename columns from EFO ids to camelCase human-readable names
        ta_id_to_name = {ta.name: ta.value for ta in ta_hierarchy}
        renamed = [
            f.col(c).alias(string2camelcase(ta_id_to_name[c])) if c in ta_id_to_name else f.col(c)
            for c in pivoted.columns
        ]
        return pivoted.select(*renamed)
