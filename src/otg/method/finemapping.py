"""Step to run study locus fine-mapping."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.types import DoubleType
from scipy import stats

from otg.common.session import Session


@dataclass
class Finemapping:
    """Fine-mapping an input study locus"""

    session: Session = Session()

    @staticmethod
    def outlier_detection(
        filtered_LDMatrix: DataFrame,
        filtered_StudyLocus: Dataframe,
        outlier_method: str,
        lead_SNP_ID: str,
        n_sample: int,
        r2_threshold: float,
        nlog10p_dentist_s_threshold: float,
        target: str,
    ) -> DataFrame:
        """Performs outlier detection using the specified method."""
        if outlier_method == "DENTIST":
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
                    & (
                        filtered_StudyLocus.nlog10p_dentist_s
                        > nlog10p_dentist_s_threshold
                    ),
                    1,
                ).otherwise(0),
            )

        elif outlier_method == "CARMA":
            # unfinished work in progress
            import carmapy.carmapy_c
            from scipy import optimize

            sumstats = pd.read_csv(
                f"{target}_locus_sumstat_flip_check.txt.gz", sep="\t"
            )
            ld = pd.read_csv(f"{target}_locus_ukbb_ld.txt.gz", sep="\t", header=None)
            outlier_tau = 0.04
            index_list = sumstats.index.tolist()
            z = sumstats["z"].tolist()
            ld_matrix = np.asmatrix(ld)
            modi_ld_S = ld_matrix

            def ridge_fun(
                x, modi_ld_S, index_list, temp_Sigma, z, outlier_tau, outlier_likelihood
            ):
                temp_ld_S = x * modi_ld_S + (1 - x) * np.eye(modi_ld_S.shape[0])
                ld_matrix[index_list[:, None], index_list] = temp_ld_S
                return outlier_likelihood(
                    index_list, ld_matrix, z, outlier_tau, len(index_list), 1
                )

            opizer = optimize(ridge_fun, interval=[0, 1], maximum=True)
            modi_ld = opizer["maximum"] * modi_ld_S + (1 - opizer["maximum"]) * np.diag(
                np.diag(modi_ld_S)
            )
            outlier_likelihood = carmapy.carmapy_c.outlier_Normal_fixed_sigma_marginal
            test_log_BF = outlier_likelihood(
                index_list, ld_matrix, z, outlier_tau, len(index_list), 1
            ) - outlier_likelihood(
                index_list, modi_ld, z, outlier_tau, len(index_list), 1
            )
            test_log_BF = -abs(test_log_BF)
            print("Outlier BF:", test_log_BF)
            print("This is xi hat:", opizer)
        else:
            pass
        return filtered_StudyLocus

    @staticmethod
    def run_finemapping(
        filtered_LDMatrix: str,
        filtered_StudyLocus: str,
        lead_SNP_ID=str,
        n_sample=int,
    ):
        """Runs the SuSiE fine-mapping algorithm from fine-mapping-inf package.
        Currently unfinished for running in ETL.
        TODO
        - Reaplce file names with filtered_LDMatrix and filtered_StudyLocus
        - Automate getting n_sample and lead_SNP_ID for a locus
        - Check susie-inf package dependencies don't cause conflicts
        """
        n_sample = self.n_sample
        susieinf_command = f"""python /Users/hn9/Documents/GitHub/fine-mapping-inf/run_fine_mapping.py \
            --sumstats /Users/hn9/Documents/GitHub/fine-mapping-inf/susieinf/loci/{lead_SNP_ID}_locus_sumstat_with_dentist.txt.gz \
            --beta-col-name beta \
            --se-col-name se \
            --ld-file /Users/hn9/Documents/GitHub/fine-mapping-inf/susieinf/loci/{lead_SNP_ID}_locus_ukbb_ld.txt.gz \
            --n {n_sample} \
            --method susieinf \
            --save-tsv \
            --eigen-decomp-prefix {lead_SNP_ID}_locus \
            --output-prefix  {lead_SNP_ID}_locus """

        subprocess.run(susieinf_command, shell=True)
        return
