"""Step to run study locus fine-mapping."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.common.session import Session

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

import math

import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

from otg.common.session import Session


@dataclass
class Dentist:
    """Dentist outlier detection

    untested as it needs study locus with R2 column (LD for all variants with lead SNP)
    """

    session: Session = Session()

    @staticmethod
    def calculate_dentist(
        filtered_StudyLocus: Dataframe,
        n_sample: int,
        r2_threshold: float,
        lead_snp_ID: str,
        nlog10p_dentist_s_threshold: float,
    ) -> DataFrame:
        """Performs outlier detection using DENTIST."""
        # need study locus summary statistics with columns: r2 with lead snp, beta, se, z
        # Calculate 'r'
        # Calculate 'r' using aggregation
        agg_result = filtered_StudyLocus.agg(
            (f.sum("R2") * n_sample).alias("R2_sum"),
            (f.count("R2") * n_sample).alias("R2_count"),
        )
        agg_result = agg_result.withColumn(
            "r", agg_result["R2_sum"] / agg_result["R2_count"]
        )
        filtered_StudyLocus = filtered_StudyLocus.crossJoin(agg_result.select("r"))

        # Find the lead SNP
        lead_idx_snp = filtered_StudyLocus.filter(
            filtered_StudyLocus.ID == lead_snp_ID
        ).first()

        # Calculate 't_dentist_s' and 'dentist_outlier'
        lead_z = lead_idx_snp.Z
        filtered_StudyLocus = filtered_StudyLocus.withColumn(
            "t_dentist_s",
            ((filtered_StudyLocus.Z - filtered_StudyLocus.r * lead_z) ** 2)
            / (1 - filtered_StudyLocus.r**2),
        )
        filtered_StudyLocus = filtered_StudyLocus.withColumn(
            "t_dentist_s",
            f.when(filtered_StudyLocus["t_dentist_s"] < 0, float("inf")).otherwise(
                filtered_StudyLocus["t_dentist_s"]
            ),
        )

        def calc_nlog10p_dentist_s(t_dentist_s):
            return math.log(1 - math.exp(-t_dentist_s)) / -math.log(10)

        udf_calc_nlog10p_dentist_s = F.udf(calc_nlog10p_dentist_s, DoubleType())
        filtered_StudyLocus = filtered_StudyLocus.withColumn(
            "nlog10p_dentist_s",
            udf_calc_nlog10p_dentist_s(filtered_StudyLocus["t_dentist_s"]),
        )

        # n_dentist_s_outlier = filtered_StudyLocus.filter((filtered_StudyLocus.R2 > r2_threshold) & (filtered_StudyLocus.nlog10p_dentist_s > nlog10p_dentist_s_threshold)).count()

        filtered_StudyLocus = filtered_StudyLocus.withColumn(
            "dentist_outlier",
            f.when(
                (filtered_StudyLocus.R2 > r2_threshold)
                & (filtered_StudyLocus.nlog10p_dentist_s > nlog10p_dentist_s_threshold),
                1,
            ).otherwise(0),
        )

        return filtered_StudyLocus
