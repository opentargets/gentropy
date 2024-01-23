"""GWAS Summary Statistics Qulity Contorl methods."""

import numpy as np
import pyspark.sql.functions as f
import pyspark.sql.types as t
import scipy as sc
from pyspark.sql import Window

from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics


class sumstat_qc:
    """Implementation of GWAS QC fucntions."""

    @staticmethod
    def _calculate_logpval(z2: float) -> float:
        """Calculate log10-pval from Z-score.

        Args:
            z2 (float): Z-score squared.

        Returns:
            float: log10-pval.

        Examples:
            >>> sumstat_qc._calculate_logpval(1.0)
            0.3010299956639812
        """
        logpval = -np.log10(sc.stats.chi2.sf((z2), 1))
        return float(logpval)

    @staticmethod
    def _logpval(pval: float) -> float:
        """Calculate log10-pval from pval.

        Args:
            pval (float): pval.

        Returns:
            float: log10-pval.

        Examples:
            >>> sumstat_qc._logpval(0.05)
            1.3010299956639813
        """
        logpval = -np.log10(pval)
        return float(logpval)

    @staticmethod
    def _calculate_lin_reg(y: list[float], x: list[float]) -> list[float]:
        """Calculate linear regression.

        Args:
            y (list[float]): y values.
            x (list[float]): x values.

        Returns:
            list[float]: slope, slope_stderr, intercept, intercept_stderr.

        Examples:
            >>> sumstat_qc._calculate_lin_reg([1,2,3], [1,2,3])
            [1.0, 0.0, 0.0, 0.0]
        """
        lin_reg = sc.stats.linregress(y, x)
        return [
            float(lin_reg.slope),
            float(lin_reg.stderr),
            float(lin_reg.intercept),
            float(lin_reg.intercept_stderr),
        ]

    @staticmethod
    def clculate_QC_GWAS(gwas_path: str, output_path: str) -> None:
        """Calculate QC metrics for GWAS.

        Args:
            gwas_path (str): Path to GWAS summary statistics.
            output_path (str): Path to output.

        Returns:
            None: None.
        """
        session = Session()

        GWAS = session.read_parquet(gwas_path, schema=SummaryStatistics.get_schema())
        linear_reg_Schema = t.StructType(
            [
                t.StructField("beta", t.FloatType(), False),
                t.StructField("beta_stderr", t.FloatType(), False),
                t.StructField("intercept", t.FloatType(), False),
                t.StructField("intercept_stderr", t.FloatType(), False),
            ]
        )
        logpval_udf = f.udf(sumstat_qc._logpval, t.DoubleType())
        calculate_logpval_udf = f.udf(sumstat_qc._calculate_logpval, t.DoubleType())
        lin_udf = f.udf(sumstat_qc._calculate_lin_reg, linear_reg_Schema)

        grp_window = Window.partitionBy("study_id")
        GWAS_columns = (
            GWAS.withColumn("zscore", f.col("beta") / f.col("se"))
            .withColumn("new_logpval", calculate_logpval_udf(f.col("zscore") ** 2))
            .withColumn("logpval", logpval_udf(f.col("pval")))
            .withColumn("var_af", 2 * (f.col("eaf") * (1 - f.col("eaf"))))
            .withColumn(
                "pheno_var",
                ((f.col("se") ** 2) * (f.col("n_total") * f.col("var_af")))
                + ((f.col("beta") ** 2) * f.col("var_af")),
            )
            .withColumn(
                "pheno_median", f.percentile_approx("pheno_var", 0.5).over(grp_window)
            )
            .withColumn(
                "N_hat",
                (
                    (f.col("pheno_median") - ((f.col("beta") ** 2) * f.col("var_af")))
                    / ((f.col("se") ** 2) * f.col("var_af"))
                ),
            )
            .groupBy("study_id")
            .agg(
                f.collect_list("logpval").alias("pval_vector"),
                f.collect_list("new_logpval").alias("new_pval_vector"),
                f.percentile_approx(f.col("N_hat") / f.col("n_total"), 0.5).alias(
                    "median_N"
                ),
                f.stddev(f.col("N_hat") / f.col("n_total")).alias("se_N"),
                f.mean(
                    "n_total",
                ).alias("N"),
                f.count("n_total").alias("total_SNP"),
                f.mean("beta").alias("mean_beta"),
            )
            .withColumn(
                "result_lin_reg",
                lin_udf(f.col("pval_vector"), f.col("new_pval_vector")),
            )
            .select(
                "study_id",
                "median_N",
                "se_N",
                "N",
                "total_SNP",
                "mean_beta",
                "result_lin_reg",
            )
        )
        GWAS_columns.write.parquet(output_path, mode="overwrite")
