"""Process SuSIE finemapping results from eQTL Catalogue."""
from dataclasses import dataclass

import pyspark.sql.functions as f
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from gentropy.common.utils import parse_pvalue
from gentropy.dataset.study_locus import StudyLocus
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex


@dataclass
class EqtlCatalogueFinemapping:
    """SuSIE finemapping dataset for eQTL Catalogue.

    Credible sets from SuSIE are extracted and transformed into StudyLocus objects:
    - A study ID is defined as a triad between: the publication, the tissue, and the measured gene (e.g. Braineac2_substantia_nigra_ENSG00000248275)
    - Each row in the `credible_sets.tsv.gz` files is represented by molecular_trait_id/variant/rsid trios. Each have their own finemapping statistics
    - log Bayes Factors are available for all variants in the `lbf_variable.txt` files
    """

    raw_credible_set_schema: StructType = StructType(
        [
            StructField("molecular_trait_id", StringType(), True),
            StructField("gene_id", StringType(), True),
            StructField("cs_id", StringType(), True),
            StructField("variant", StringType(), True),
            StructField("rsid", StringType(), True),
            StructField("cs_size", IntegerType(), True),
            StructField("pip", DoubleType(), True),
            StructField("pvalue", DoubleType(), True),
            StructField("beta", DoubleType(), True),
            StructField("se", DoubleType(), True),
            StructField("z", DoubleType(), True),
            StructField("cs_min_r2", DoubleType(), True),
            StructField("region", StringType(), True),
        ]
    )
    raw_lbf_schema: StructType = StructType(
        [
            StructField("molecular_trait_id", StringType(), True),
            StructField("region", StringType(), True),
            StructField("variant", StringType(), True),
            StructField("chromosome", StringType(), True),
            StructField("position", IntegerType(), True),
            StructField("lbf_variable1", DoubleType(), True),
            StructField("lbf_variable2", DoubleType(), True),
            StructField("lbf_variable3", DoubleType(), True),
            StructField("lbf_variable4", DoubleType(), True),
            StructField("lbf_variable5", DoubleType(), True),
            StructField("lbf_variable6", DoubleType(), True),
            StructField("lbf_variable7", DoubleType(), True),
            StructField("lbf_variable8", DoubleType(), True),
            StructField("lbf_variable9", DoubleType(), True),
            StructField("lbf_variable10", DoubleType(), True),
        ]
    )

    @classmethod
    def _extract_credible_set_index(
        cls: type[EqtlCatalogueFinemapping], cs_id: Column
    ) -> Column:
        """Extract the credible set index from the cs_id."""
        return f.split(cs_id, "_L")[1].cast(IntegerType())

    @classmethod
    def _extract_dataset_id_from_file_path(
        cls: type[EqtlCatalogueFinemapping], file_path: Column
    ) -> Column:
        """Extract the dataset_id from the file_path. The dataset_id follows the pattern QTD{6}.

        Args:
            file_path: The file path.

        Returns:
            The dataset_id.

        Examples:
            >>> extract_dataset_id_from_file_path(f.lit("QTD000046.credible_sets.tsv")).show()
            +----------+
            |dataset_id|
            +----------+
            | QTD000046|
            +----------+
        """
        return f.regexp_extract(file_path, r"QTD\d{6}", 0).alias("dataset_id")

    @classmethod
    def parse_susie_results(
        cls: type[EqtlCatalogueFinemapping],
        credible_sets: DataFrame,
        lbf: DataFrame,
        studies_metadata: DataFrame,
    ) -> DataFrame:
        """Parse the SuSIE results into a DataFrame containing the finemapping statistics and metadata about the studies."""
        ss_ftp_path_template = "https://ftp.ebi.ac.uk/pub/databases/spot/eQTL/sumstats"
        return (
            credible_sets.withColumn(
                "dataset_id",
                cls._extract_dataset_id_from_file_path(
                    f.input_file_name()
                ),
            )
            # filter out credible sets from any method other than gene quantification
            .join(
                studies_metadata.filter(f.col("quant_method") == "ge"), on="dataset_id"
            )
            # bring in the lbf data
            .join(lbf, on=["molecular_trait_id", "region", "variant"])
            .withColumn(
                "logBF",
                f.when(f.col("credibleSetIndex") == 1, f.col("lbf_variable1"))
                .when(f.col("credibleSetIndex") == 2, f.col("lbf_variable2"))
                .when(f.col("credibleSetIndex") == 3, f.col("lbf_variable3"))
                .when(f.col("credibleSetIndex") == 4, f.col("lbf_variable4"))
                .when(f.col("credibleSetIndex") == 5, f.col("lbf_variable5"))
                .when(f.col("credibleSetIndex") == 6, f.col("lbf_variable6"))
                .when(f.col("credibleSetIndex") == 7, f.col("lbf_variable7"))
                .when(f.col("credibleSetIndex") == 8, f.col("lbf_variable8"))
                .when(f.col("credibleSetIndex") == 9, f.col("lbf_variable9"))
                .when(f.col("credibleSetIndex") == 10, f.col("lbf_variable10")),
            )
            .select(
                f.regexp_replace(f.col("variant"), r"chr", "").alias("variantId"),
                f.col("region"),
                f.col("chromosome"),
                f.col("position"),
                f.col("pip").alias("posteriorProbability"),
                *parse_pvalue(f.col("pvalue")),
                f.col("sample_size").alias("nSamples"),
                f.col("beta"),
                f.col("se").alias("standardError"),
                cls._extract_credible_set_index(
                    f.col("cs_id")
                ).alias("credibleSetIndex"),
                f.col("logBF"),
                f.lit("SuSie").alias("finemappingMethod"),
                # Study metadata
                f.col("gene_id").alias("geneId"),
                f.array(f.col("molecular_trait_id")).alias("traitFromSourceMappedIds"),
                f.col("dataset_id"),
                f.concat_ws(
                    "_", f.col("study_label"), f.col("sample_group"), f.col("gene_id")
                ).alias("studyId"),
                f.col("tissue_id").alias("c"),
                EqtlCatalogueStudyIndex._identify_study_type(
                    f.col("study_label")
                ).alias("studyType"),
                f.col("study_label").alias("projectId"),
                f.concat_ws(
                    "/",
                    f.lit(ss_ftp_path_template),
                    f.col("study_id"),
                    f.col("dataset_id"),
                ).alias("summarystatsLocation"),
                f.lit(True).alias("hasSumstats"),
            )
            .withColumn(
                "studyLocusId",
                StudyLocus.assign_study_locus_id(f.col("studyId"), f.col("variantId")),
            )
            .persist()
        )

    @classmethod
    def from_susie_results(
        cls: type[EqtlCatalogueFinemapping], processed_finngen_finemapping_df: DataFrame
    ) -> StudyLocus:
        """Create a StudyLocus object from the processed SuSIE results."""
        lead_w = Window.partitionBy("studyId", "region", "credibleSetIndex")
        study_locus_cols = [
            field.name
            for field in StudyLocus.get_schema().fields
            if field.name in processed_finngen_finemapping_df.columns
        ] + ["locus"]
        return StudyLocus(
            _df=(
                processed_finngen_finemapping_df.withColumn(
                    "isLead",
                    f.row_number().over(
                        lead_w.orderBy(
                            *[
                                f.col("pValueExponent").asc(),
                                f.col("pValueMantissa").asc(),
                            ]
                        )
                    )
                    == f.lit(1),
                )
                .withColumn(
                    # Collecting all variants that constitute the credible set brings as many variants as the credible set size
                    "locus",
                    f.collect_set(
                        f.struct(
                            "variantId",
                            "posteriorProbability",
                            "pValueMantissa",
                            "pValueExponent",
                            "logBF",
                            "beta",
                            "standardError",
                        )
                    ).over(lead_w),
                )
                .filter(f.col("isLead"))
                .select(study_locus_cols)
            ),
            _schema=StudyLocus.get_schema(),
        ).annotate_credible_sets()
