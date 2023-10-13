"""Data extraction for fine-mapping."""

from __future__ import annotations

from dataclasses import dataclass

from typing import TYPE_CHECKING
from otg.common.session import Session
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType


import hail as hl
import pandas as pd
import numpy as np
import pyspark.sql.functions as f
from hail.linalg import BlockMatrix
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit
from pyspark.sql.functions import array
from typing import Tuple



@dataclass
class FMDataExtraction(Dataset):
    """LD Matrix extraction from gnomad and subsequent SNP matching with a study locus"""

    session: Session = Session()

    def get_FM_study_locus(self: FMDataExtraction, sumstats: DataFrame) -> Tuple[DataFrame, list]:
        """Get summary statistics for a locus.
        
        Args:
            sumstats (DataFrame): summary statistics dataframe to select locus from (currently using all UKBB as an example)

        Returns:
            lead_SNP_id (str): The lead SNP ID as a string in the format chr1:1234:A:T (needed for hail GRCh38 contigs).
            fm_study_locus (DataFrame): A filtered DataFrame containing summary statistics for a specific locus.
    
        """
        # Getting row with the smallest pValueExponent (getting an example lead SNP)
        smallest_pValue_row = sumstats.orderBy(col("pValueExponent")).first()
        lead_SNP_id = smallest_pValue_row['variantId']
        # needs chr and : to match GRCh38 hail format
        lead_SNP_id = "chr" + lead_SNP_id.replace("_", ":")
        chromosome_value = smallest_pValue_row['chromosome']
        position_value = smallest_pValue_row['position']
        study_id = smallest_pValue_row['studyId']

        position_lower = position_value - 500000
        position_upper = position_value + 500000

        fm_study_locus = sumstats.filter(
            (col("studyId") == study_id) &
            (col("chromosome") == chromosome_value) &
            (col("position") >= position_lower) &
            (col("position") <= position_upper)
)

        return lead_SNP_id, fm_study_locus

    def get_gnomad_ld_matrix(self: FMDataExtraction, lead_SNP_id: str) -> DataFrame, list:
        """Get LD matrix from gnomad for a locus using lead SNP ID and then liftover for GRCh38.

        Args:
            lead_SNP_id (str): The lead SNP identifier in the format "chr1:1234:A:T" (needed for hail GRCh38 contigs).

        Returns:
                unfiltered_LDMatrix (DataFrame): The LD matrix for the locus region.
                SNP_ids_38 (list): A list of SNP identifiers in the LD matrix, lifted over to GRCh38.
        """
        rg38 = hl.get_reference("GRCh38")
        rg37 = hl.get_reference("GRCh37")
        rg37.add_liftover(
            "gs://hail-common/references/grch37_to_grch38.over.chain.gz", rg38
        )
        rg38.add_liftover(
            "gs://hail-common/references/grch38_to_grch37.over.chain.gz", rg37
        )
        bm = BlockMatrix.read(
            "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.nfe.common.adj.ld.bm"
        )
        variant_table = hl.read_table(
            "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.nfe.common.adj.ld.variant_indices.ht"
        )
        # Liftover lead SNP ID to GRCh37
        locus = hl.parse_variant(lead_SNP_id, reference_genome="GRCh38").locus
        temp_table = hl.Table.parallelize([hl.struct(locus=locus)])
        locus_values = temp_table.select("locus").collect()
        locus_value = locus_values[0].locus
        contig = locus_value.contig
        position = locus_value.position
        locus_37 = hl.liftover(hl.locus(contig, position, "GRCh38"), "GRCh37")

        # Get LD matrix from gnomad
        window_size = 500000
        locus_variants = variant_table.filter(
            (hl.abs(variant_table.locus.position - locus_37.position) <= window_size)
            & (variant_table.locus.contig == locus_37.contig)
        )
        indices = locus_variants["idx"].collect()
        sub_bm = bm.filter(indices, indices)
        numpy_array = sub_bm.to_numpy()
        # Make triangular values from gnomad symmetrical (getting full values in matrix)
        n = numpy_array.shape[0]
        lower_triangle_mask = np.tril(np.ones((n, n), dtype=bool), k=-1)
        numpy_array += numpy_array.T * lower_triangle_mask
        ld_df = pd.DataFrame(numpy_array)
        # need to change to iteritems due to old pandas version error
        ld_df.iteritems = ld_df.items

        # Get a list of SNP IDs in the matrix in GRCh38
        locus_variants = locus_variants.annotate(
            locus_38=hl.liftover(locus_variants.locus, "GRCh38")
        )
        locus_variants = locus_variants.annotate(
            snp_id_38=hl.str(locus_variants.locus_38.contig)
            + "_"
            + hl.str(locus_variants.locus_38.position)
            + "_"
            + locus_variants.alleles[0]
            + "_"
            + locus_variants.alleles[1]
        )
        SNP_ids_38 = locus_variants["snp_id_38"].collect()
        ld_df.columns = SNP_ids_38
        unfiltered_LDMatrix = self.session.spark.createDataFrame(ld_df)

        return unfiltered_LDMatrix, SNP_ids_38

    def get_matching_snps(
        self: FMDataExtraction,
        unfiltered_LDMatrix: DataFrame,
        fm_study_locus: DataFrame,
        SNP_ids_38: list,
    ) -> Tuple[DataFrame, DataFrame]:
        """Filter for matching SNPs between StudyLocus and LD Matrix.

        Args:
            unfiltered_LDMatrix (DataFrame): A DataFrame representing the unfiltered LD matrix.
            fm_study_locus (DataFrame): A DataFrame containing summary statistics for a specific locus.
            SNP_ids_38 (list): A list of SNP identifiers in GRCh38.

        Returns:
            Tuple[DataFrame, DataFrame]: A tuple containing the following:
                - fm_filtered_LDMatrix (DataFrame): A DataFrame containing matching SNPs shared between LD matrix and locus.
                - fm_filtered_StudyLocus (DataFrame): A DataFrame containing matching SNPs shared between locus and LD matrix.
        """
        # Filtering sumstats versus SNPs in LD matrix region
        snp_ids_38_set = set(SNP_ids_38)
        fm_filtered_StudyLocus = fm_study_locus.filter(col("variantId").isin(snp_ids_38_set))

        # Extract the list of unique variant IDs from filtered_study_locus
        unique_variant_ids = [row.variantId for row in fm_filtered_StudyLocus.select("variantId").distinct().collect()]

        # Create a new column 'variantId' with an array containing the column names
        column_names = unfiltered_LDMatrix.columns
        ld_df = ld_df.withColumn('variantId', array(column_names))

        # Define the columns to keep in the LD matrix
        columns_to_keep = ["variantId"] + [col_name for col_name in ld_df.columns if col_name in unique_variant_ids]

        # Select the desired columns in the LD matrix
        filtered_LDMatrix = ld_df.select(*columns_to_keep)

        # Alias the 'variantId' column to match the column name
        filtered_LDMatrix = filtered_LDMatrix.withColumnRenamed('variantId', 'variantIdCol')

        # Filter rows in the LD matrix to keep only those present in study_locus
        filtered_LDMatrix = filtered_LDMatrix.filter(filtered_LDMatrix['variantIdCol'].cast('string').isin(unique_variant_ids))
        fm_filtered_LDMatrix = filtered_LDMatrix.drop("variantIdCol")
        return fm_filtered_LDMatrix, fm_filtered_StudyLocus
