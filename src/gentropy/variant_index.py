"""Step to generate variant index dataset."""

from __future__ import annotations

import pyspark.sql.functions as f

from gentropy.common.session import Session
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.variant_annotation import VariantAnnotation
from gentropy.dataset.variant_index import VariantIndex


class VariantIndexStep:
    """Run variant index step to only variants in study-locus sets.

    Using a `VariantAnnotation` dataset as a reference, this step creates and writes a dataset of the type `VariantIndex` that includes only variants that have disease-association data with a reduced set of annotations.
    """

    def __init__(
        self: VariantIndexStep,
        session: Session,
        variant_annotation_path: str,
        credible_set_path: str,
        variant_index_path: str,
    ) -> None:
        """Run VariantIndex step.

        Args:
            session (Session): Session object.
            variant_annotation_path (str): Variant annotation dataset path.
            credible_set_path (str): Credible set dataset path.
            variant_index_path (str): Variant index dataset path.
        """
        # Extract
        va = VariantAnnotation.from_parquet(session, variant_annotation_path)
        credible_set = StudyLocus.from_parquet(
            session, credible_set_path, recursiveFileLookup=True
        )

        # Transform
        vi = VariantIndex.from_variant_annotation(va, credible_set)

        (
            vi.df.write.partitionBy("chromosome")
            .mode(session.write_mode)
            .parquet(variant_index_path)
        )


class ConvertToVcfStep:
    """Convert dataset with variant annotation to VCF step."""

    def __init__(
        self,
        session: Session,
        source_path: str,
        source_format: str,
        vcf_path: str,
    ) -> None:
        """Initialize step.

        Args:
            session (Session): Session object.
            source_path (str): Input dataset path.
            source_format(str): Format of the input dataset.
            vcf_path (str): Output VCF file path.
        """
        df = session.load_data(source_path, source_format).limit(100)
        if rsids_to_map := (
            df.filter(
                (f.col("variantId").isNull()) & (f.col("variantRsId").isNotNull())
            )
            .select("variantRsId")
            .distinct()
            .toPandas()["variantRsId"]
            .to_list()
        ):
            rsid_to_variantids = VariantIndex.fetch_coordinates(rsids_to_map)
            mapping_df = session.spark.createDataFrame(
                rsid_to_variantids.items(), schema=["variantRsId", "mappedVariantIds"]
            ).select(
                "variantRsId", f.explode("mappedVariantIds").alias("mappedVariantId")
            )
            df = (
                df.join(mapping_df, "variantRsId", "left")
                .withColumn(
                    "variantId",
                    f.coalesce(f.col("variantId"), f.col("mappedVariantId")),
                )
                .drop("mappedVariantId")
            )
        else:
            df

        # Handle missing rsIDs (optional)
        rsid_expr = (
            f.coalesce(f.col("variantRsId"), f.lit("."))
            if "variantRsId" in df.columns
            else f.lit(".")
        )
        vcf = (
            df.filter(f.col("variantId").isNotNull())
            .select(
                f.coalesce(f.split(f.col("variantId"), "_")[0], f.lit(".")).alias(
                    "#CHROM"
                ),
                f.coalesce(f.split(f.col("variantId"), "_")[1], f.lit("."))
                .cast("int")
                .alias("POS"),
                rsid_expr.alias("ID"),
                f.coalesce(f.split(f.col("variantId"), "_")[2], f.lit(".")).alias(
                    "REF"
                ),
                f.coalesce(f.split(f.col("variantId"), "_")[3], f.lit(".")).alias(
                    "ALT"
                ),
                f.lit(".").alias("QUAL"),
                f.lit(".").alias("FILTER"),
                f.lit(".").alias("INFO"),
            )
            .filter(f.col("#CHROM") != ".")
            .orderBy(f.col("#CHROM").asc(), f.col("POS").asc())
            .distinct()
        )
        # Load
        header = "##fileformat=VCFv4.3"
        data_header = "\t".join(vcf.columns)

        with open(vcf_path, "w") as file:
            file.write(header)
            file.write("\n")
            file.write(data_header)
            file.write("\n")
            for row in vcf.collect():
                file.write("\t".join(map(str, row)) + "\n")
