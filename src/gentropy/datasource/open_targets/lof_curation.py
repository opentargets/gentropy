"""Parser for Loss-of-Function variant data from Open Targets Project OTAR2075."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from gentropy.common.spark_helpers import enforce_schema
from gentropy.dataset.variant_index import InSilicoPredictorNormaliser, VariantIndex

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


class OpenTargetsLOF:
    """Class to parse Loss-of-Function variant data from Open Targets Project OTAR2075."""

    IN_SILICO_PREDICTOR_SCHEMA = VariantIndex.get_schema()[
        "inSilicoPredictors"
    ].dataType.elementType

    @staticmethod
    @enforce_schema(IN_SILICO_PREDICTOR_SCHEMA)
    def _get_lof_assessment(verdict: Column) -> Column:
        """Get curated Loss-of-Function assessment from verdict column.

        Args:
            verdict (Column): verdict column from the input dataset.

        Returns:
            Column: struct following the in silico predictor schema.
        """
        return f.struct(
            f.lit("LossOfFunctionCuration").alias("method"),
            verdict.alias("assessment"),
        )

    @classmethod
    def as_variant_index(
        cls: type[OpenTargetsLOF],
        lof_dataset: DataFrame
    ) -> VariantIndex:
        """Ingest Loss-of-Function information as a VariantIndex object.

        Args:
            lof_dataset (DataFrame): curated input dataset from OTAR2075.

        Returns:
            VariantIndex: variant annotations with loss-of-function assessments.
        """
        return VariantIndex(
            _df=(
                lof_dataset
                .select(
                    f.from_csv(f.col("Variant ID GRCh37"), "chr string, pos string, ref string, alt string", {"sep": "-"}).alias("h37"),
                    f.from_csv(f.col("Variant ID GRCh38"), "chr string, pos string, ref string, alt string", {"sep": "-"}).alias("h38"),
                    "Verdict"
                )
                .select(
                    # As some GRCh37 variants do not correctly lift over to the correct GRCh38 variant,
                    # chr_pos is taken from the GRCh38 variant id, and ref_alt from the GRCh37 variant id
                    f.concat_ws("_", f.col("h38.chr"), f.col("h38.pos"), f.col("h37.ref"), f.col("h37.alt")).alias("variantId"),
                    # Mandatory fields for VariantIndex:
                    f.col("h38.chr").alias("chromosome"),
                    f.col("h38.pos").cast(t.IntegerType()).alias("position"),
                    f.col("h37.ref").alias("referenceAllele"),
                    f.col("h37.alt").alias("alternateAllele"),
                    # Populate inSilicoPredictors field:
                    f.array(cls._get_lof_assessment(f.col("Verdict"))).alias("inSilicoPredictors"),
                )
                # Convert assessments to normalised scores:
                .withColumn("inSilicoPredictors", InSilicoPredictorNormaliser.normalise_in_silico_predictors(f.col("inSilicoPredictors")))
            ),
            _schema=VariantIndex.get_schema(),
        )
