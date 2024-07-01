"""Step to generate variant index dataset."""

from __future__ import annotations

import pyspark.sql.functions as f

from gentropy.common.session import Session
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.ensembl.vep_parser import VariantEffectPredictorParser


class VariantIndexStep:
    """Generate variant index based on a VEP output in json format.

    The variant index is a dataset that contains variant annotations extracted from VEP output. It is expected that all variants in the VEP output are present in the variant index.
    There's an option to provide extra variant annotations to be added to the variant index eg. allele frequencies from GnomAD.
    """

    def __init__(
        self: VariantIndexStep,
        session: Session,
        vep_output_json_path: str,
        variant_index_path: str,
        gnomad_variant_annotations_path: str | None = None,
    ) -> None:
        """Run VariantIndex step.

        Args:
            session (Session): Session object.
            vep_output_json_path (str): Variant effect predictor output path (in json format).
            variant_index_path (str): Variant index dataset path to save resulting data.
            gnomad_variant_annotations_path (str | None): Path to extra variant annotation dataset.
        """
        # Extract variant annotations from VEP output:
        variant_index = VariantEffectPredictorParser.extract_variant_index_from_vep(
            session.spark, vep_output_json_path
        )

        # Process variant annotations if provided:
        if gnomad_variant_annotations_path:
            # Read variant annotations from parquet:
            annotations = VariantIndex.from_parquet(
                session=session,
                path=gnomad_variant_annotations_path,
                recursiveFileLookup=True,
            )

            # Update file with extra annotations:
            variant_index = variant_index.add_annotation(annotations)

        (
            variant_index.df.write.partitionBy("chromosome")
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
