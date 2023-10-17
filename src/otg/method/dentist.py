"""Step to run study locus fine-mapping."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from otg.common.session import Session

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

import numpy as np
import pyspark.sql.functions as f
from pyspark.sql.types import DoubleType
from scipy import stats

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
        filtered_StudyLocus = filtered_StudyLocus.withColumn(
            "r", (f.sum("R2") * n_sample) / (f.count("R2") * n_sample)
        )

        lead_idx_snp_row = filtered_StudyLocus.filter(
            filtered_StudyLocus.ID == lead_snp_ID
        ).collect()[0]
        lead_z = lead_idx_snp_row.beta / lead_idx_snp_row.se

        # 2. Calculate 't_dentist_s' and 'dentist_outlier'
        filtered_StudyLocus = filtered_StudyLocus.withColumn(
            "t_dentist_s",
            (
                (
                    filtered_StudyLocus.beta / filtered_StudyLocus.se
                    - filtered_StudyLocus.r * lead_z
                )
                ** 2
            )
            / (1 - filtered_StudyLocus.r**2),
        )
        filtered_StudyLocus = filtered_StudyLocus.withColumn(
            "t_dentist_s",
            f.when(filtered_StudyLocus["t_dentist_s"] < 0, float("inf")).otherwise(
                filtered_StudyLocus["t_dentist_s"]
            ),
        )

        def calc_nlog10p_dentist_s(t_dentist_s):
            return stats.chi2.logsf(t_dentist_s, df=1) / -np.log(10)

        calc_nlog10p_dentist_s_udf = f.udf(calc_nlog10p_dentist_s, DoubleType())
        filtered_StudyLocus = filtered_StudyLocus.withColumn(
            "nlog10p_dentist_s", calc_nlog10p_dentist_s_udf("t_dentist_s")
        )

        # Count the number of DENTIST outliers and creating new column
        n_dentist_s_outlier = filtered_StudyLocus.filter(
            (filtered_StudyLocus.R2 > r2_threshold)
            & (filtered_StudyLocus.nlog10p_dentist_s > nlog10p_dentist_s_threshold)
        ).count()
        print(f"Number of DENTIST outliers detected: {n_dentist_s_outlier}")
        filtered_StudyLocus = filtered_StudyLocus.withColumn(
            "dentist_outlier",
            f.when(
                (filtered_StudyLocus.R2 > r2_threshold)
                & (filtered_StudyLocus.nlog10p_dentist_s > nlog10p_dentist_s_threshold),
                1,
            ).otherwise(0),
        )
        return filtered_StudyLocus
