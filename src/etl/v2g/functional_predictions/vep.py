"""The variant annotation dataset contains information about the impact of a variant on a transcript or protein. These can be mapped to genes allowing us to establish significant relationships between variants and genes."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from etl.common.spark_helpers import get_record_with_maximum_value

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl.common.ETLSession import ETLSession


def main(
    etl: ETLSession,
    variant_index: DataFrame,
    variant_annotation: DataFrame,
    variant_consequence_lut_path: str,
) -> tuple[DataFrame, ...]:
    """Extracts variant to gene assignments for the variants included in the index and the features predicted by VEP.

    Args:
        etl (ETLSession): ETL session,
        variant_index (DataFrame): DataFrame with the OTG variant index
        variant_annotation (DataFrame): Dataframe with the annotated variants
        variant_consequence_lut_path (str): The path to the LUT between the functional consequences and their assigned V2G score

    Returns:
        DataFrame: variant to gene assignments from VEP
    """
    etl.logger.info("Parsing functional predictions...")
    annotated_variants = (
        variant_annotation.select(
            "variantId",
            "chromosome",
            # exploding the array already removes record without VEP annotation
            f.explode("vep.transcriptConsequences").alias("transcriptConsequence"),
        )
        .join(
            variant_index.select("variantId", "chromosome"),
            on=["variantId", "chromosome"],
            how="inner",
        )
        .persist()
    )

    variant_consequence_lut = read_consequence_lut(etl, variant_consequence_lut_path)
    vep_consequences = get_variant_consequences(
        annotated_variants, variant_consequence_lut
    )
    etl.logger.info("Extracted functional consequence from VEP.")
    vep_polyphen = get_polyphen_score(annotated_variants)
    etl.logger.info("Extracted polyphen scores from VEP.")
    vep_sift = get_sift_score(annotated_variants)
    etl.logger.info("Extracted sift scores from VEP.")
    vep_plof = get_plof_flag(annotated_variants)
    etl.logger.info("Extracted pLOF assesments from LOFTEE.")

    return vep_consequences, vep_polyphen, vep_sift, vep_plof


def read_consequence_lut(
    etl: ETLSession, variant_consequence_lut_path: str
) -> DataFrame:
    """Reads the variant consequence LUT from the given path.

    Args:
        etl (ETLSession): ETL session
        variant_consequence_lut_path (str): Path to the table with the variant consequences sorted by severity

    Returns:
        DataFrame: variant consequence LUT
    """
    return etl.spark.read.csv(
        variant_consequence_lut_path, sep="\t", header=True
    ).select(
        f.element_at(f.split("Accession", r"/"), -1).alias(
            "variantFunctionalConsequenceId"
        ),
        f.col("Term").alias("label"),
        f.col("v2g_score").cast("double").alias("score"),
    )


def get_variant_consequences(
    variants_df: DataFrame,
    variant_consequence_lut: DataFrame,
) -> DataFrame:
    """Creates a dataset with variant to gene assignments based on VEP's predicted consequence on the transcript.

    Args:
        variants_df (DataFrame): Dataframe with two columns: "id" and "transcriptConsequence"
        variant_consequence_lut (DataFrame): Dataframe with the variant consequences sorted by severity

    Returns:
        DataFrame: High and medium severity variant to gene assignments
    """
    return (
        variants_df.select(
            "variantId",
            "chromosome",
            f.col("transcriptConsequence.gene_id").alias("geneId"),
            f.explode("transcriptConsequence.consequence_terms").alias("label"),
            f.lit("vep").alias("datatypeId"),
            f.lit("variantConsequence").alias("datasourceId"),
        )
        # A variant can have multiple predicted consequences on a transcript, the most severe one is selected
        .join(
            f.broadcast(variant_consequence_lut),
            on="label",
            how="inner",
        )
        .filter(f.col("score") != 0)
        .transform(
            lambda df: get_record_with_maximum_value(
                df, ["variantId", "geneId"], "score"
            )
        )
    )


def get_polyphen_score(
    variants_df: DataFrame,
) -> DataFrame:
    """Creates a dataset with variant to gene assignments with a PolyPhen's predicted score on the transcript.

    Polyphen informs about the probability that a substitution is damaging.

    Args:
        variants_df (DataFrame): Dataframe with two columns: "id" and "transcriptConsequence"

    Returns:
        DataFrame: variant to gene assignments with their polyphen scores
    """
    return variants_df.filter(
        f.col("transcriptConsequence.polyphen_score").isNotNull()
    ).select(
        "variantId",
        "chromosome",
        f.col("transcriptConsequence.gene_id").alias("geneId"),
        f.col("transcriptConsequence.polyphen_score").alias("score"),
        f.col("transcriptConsequence.polyphen_prediction").alias("label"),
        f.lit("vep").alias("datatypeId"),
        f.lit("polyphen").alias("datasourceId"),
    )


def get_sift_score(
    variants_df: DataFrame,
) -> DataFrame:
    """Creates a dataset with variant to gene assignments with a SIFT's predicted score on the transcript.

    SIFT informs about the probability that a substitution is tolerated so scores nearer zero are more likely to be deleterious.

    Args:
        variants_df (DataFrame): Dataframe with two columns: "id" and "transcriptConsequence"

    Returns:
        DataFrame: variant to gene assignments with their SIFT scores
    """
    return variants_df.filter(
        f.col("transcriptConsequence.sift_score").isNotNull()
    ).select(
        "variantId",
        "chromosome",
        f.col("transcriptConsequence.gene_id").alias("geneId"),
        f.col("transcriptConsequence.sift_score").alias("resourceScore"),
        f.expr("1 - transcriptConsequence.sift_score").alias("score"),
        f.col("transcriptConsequence.sift_prediction").alias("label"),
        f.lit("vep").alias("datatypeId"),
        f.lit("sift").alias("datasourceId"),
    )


def get_plof_flag(variants_df: DataFrame) -> DataFrame:
    """Creates a dataset with variant to gene assignments with a flag indicating if the variant is predicted to be a loss-of-function variant by the LOFTEE algorithm.

    Args:
        variants_df (DataFrame): Dataframe with two columns: "id" and "transcriptConsequence"

    Returns:
        DataFrame: variant to gene assignments from the LOFTEE algorithm
    """
    return (
        variants_df.filter(f.col("transcriptConsequence.lof").isNotNull())
        .withColumn(
            "isHighQualityPlof",
            f.when(f.col("transcriptConsequence.lof") == "HC", True).when(
                f.col("transcriptConsequence.lof") == "LC", False
            ),
        )
        .withColumn(
            "score",
            f.when(f.col("isHighQualityPlof"), 1.0).when(
                ~f.col("isHighQualityPlof"), 0
            ),
        )
        .select(
            "variantId",
            "chromosome",
            f.col("transcriptConsequence.gene_id").alias("geneId"),
            "isHighQualityPlof",
            f.col("score"),
            f.lit("vep").alias("datatypeId"),
            f.lit("loftee").alias("datasourceId"),
        )
    )
