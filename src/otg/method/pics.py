"""Calculate PICS for a given study and locus."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window
from scipy.stats import norm

if TYPE_CHECKING:
    from pyspark.sql import Column

    from otg.dataset.study_locus import StudyLocus


class PICS:
    """Probabilistic Identification of Causal SNPs (PICS), an algorithm estimating the probability that an individual variant is causal considering the haplotype structure and observed pattern of association at the genetic locus."""

    @staticmethod
    @f.udf(t.DoubleType())
    def _norm_sf(mu: float, std: float, neglog_p: float) -> float | None:
        """Returns the survival function of the normal distribution for the p-value.

        Args:
            mu (float): mean
            std (float): standard deviation
            neglog_p (float): negative log p-value

        Returns:
            float: survival function

        Examples:
            >>> d = [{"mu": 0, "neglog_p": 0, "std": 1}, {"mu": 1, "neglog_p": 10, "std": 10}]
            >>> spark.createDataFrame(d).withColumn("norm_sf", PICS._norm_sf(f.col("mu"), f.col("std"), f.col("neglog_p"))).show()
            +---+--------+---+-------------------+
            | mu|neglog_p|std|            norm_sf|
            +---+--------+---+-------------------+
            |  0|       0|  1|                1.0|
            |  1|      10| 10|0.36812025069351895|
            +---+--------+---+-------------------+
            <BLANKLINE>
        """
        try:
            return float(norm(mu, std).sf(neglog_p) * 2)
        except TypeError:
            return None

    @staticmethod
    def _is_in_credset(
        study_id: Column,
        variant_id: Column,
        pics_postprob: Column,
        credset_probability: float,
    ) -> Column:
        """Check whether a variant is in the XX% credible set.

        Args:
            study_id (Column): Study ID column
            variant_id (Column): Variant ID column
            pics_postprob (Column): PICS posterior probability column
            credset_probability (float): Credible set probability

        Returns:
            Column: Whether the variant is in the credible set

        Examples:
            >>> d = [
            ... {"study_id": "1", "variant_id": "1", "pics_postprob": 1.0},
            ... {"study_id": "1", "variant_id": "1", "pics_postprob": 0.9}]
            >>> df = spark.createDataFrame(d)
            >>> df.withColumn("is_in_credset", PICS._is_in_credset(f.col("study_id"), f.col("variant_id"), f.col("pics_postprob"), 0.95)).show()
            +-------------+--------+----------+-------------+
            |pics_postprob|study_id|variant_id|is_in_credset|
            +-------------+--------+----------+-------------+
            |          1.0|       1|         1|         true|
            |          0.9|       1|         1|        false|
            +-------------+--------+----------+-------------+
            <BLANKLINE>

        """
        w_cumlead = (
            Window.partitionBy(study_id, variant_id)
            .orderBy(f.desc(pics_postprob))
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        pics_postprob_cumsum = f.sum(pics_postprob).over(w_cumlead)
        w_credset = Window.partitionBy(study_id, variant_id).orderBy(
            pics_postprob_cumsum
        )
        return (
            # If there is only one row and the posterior probability meets the criteria, the flag is True:
            f.when(
                (f.count(pics_postprob_cumsum).over(w_credset) == 1)
                & (pics_postprob_cumsum >= credset_probability),
                True,
            )
            # If the posterior probability meets the criteria the flag is True:
            .when(
                f.lag(pics_postprob_cumsum, 1).over(w_credset) >= credset_probability,
                False,
            )
            # If criteria is not met (posterior probability is null), flag is False:
            .otherwise(False)
        )

    @staticmethod
    def _pics_posterior_probability(
        study_locus_id: Column,
        neglog_p: Column,
        r: Column,
        k: float,
    ) -> Column:
        """Compute the PICS posterior probability.

        Args:
            study_locus_id (Column): Study-Locus ID required for windowing purposes
            neglog_p (Column): Negative log p-value
            r (Column): R-squared
            k (float): Empiric constant that can be adjusted to fit the curve, 6.4 recommended.

        Returns:
            Column: PICS posterior probability
        """
        w_lead = Window.partitionBy(study_locus_id)

        pics_mu = PICS._pics_mu(neglog_p, r)
        pics_std = PICS._pics_standard_deviation(neglog_p, r, k)

        pics_relative_prob = f.when(pics_std == 0, 1.0).otherwise(
            PICS._norm_sf(pics_mu, pics_std, neglog_p)
        )
        return pics_relative_prob / f.sum(pics_relative_prob).over(w_lead)

    @staticmethod
    def _pics_standard_deviation(neglog_p: Column, r: Column, k: float) -> Column:
        """Compute the PICS standard deviation.

        Args:
            neglog_p (Column): Negative log p-value
            r (Column): R-squared
            k (float): Empiric constant that can be adjusted to fit the curve, 6.4 recommended.

        Returns:
            Column: PICS standard deviation

        Examples:
            >>> k = 6.4
            >>> d = [(1.0, 1.0), (10.0, 1.0), (10.0, 0.5), (100.0, 0.5), (1.0, 0.0)]
            >>> spark.createDataFrame(d).toDF("neglog_p", "r").withColumn("std", PICS._pics_standard_deviation(f.col("neglog_p"), f.col("r"), k)).show()
            +--------+---+-----------------+
            |neglog_p|  r|              std|
            +--------+---+-----------------+
            |     1.0|1.0|              0.0|
            |    10.0|1.0|              0.0|
            |    10.0|0.5|1.571749395040553|
            |   100.0|0.5|4.970307999319905|
            |     1.0|0.0|              0.5|
            +--------+---+-----------------+
            <BLANKLINE>
        """
        return f.sqrt(1 - f.abs(r) ** k) * f.sqrt(neglog_p) / 2

    @staticmethod
    def _pics_mu(neglog_p: Column, r: Column) -> Column:
        """Compute the PICS mu.

        Args:
            neglog_p (Column): Negative log p-value
            r (Column): R

        Returns:
            Column: PICS mu

        Examples:
            >>> d = [(1.0, 1.0), (10.0, 1.0), (10.0, 0.5), (100.0, 0.5), (1.0, 0.0)]
            >>> spark.createDataFrame(d).toDF("neglog_p", "r").withColumn("mu", PICS._pics_mu(f.col("neglog_p"), f.col("r"))).show()
            +--------+---+----+
            |neglog_p|  r|  mu|
            +--------+---+----+
            |     1.0|1.0| 1.0|
            |    10.0|1.0|10.0|
            |    10.0|0.5| 2.5|
            |   100.0|0.5|25.0|
            |     1.0|0.0| 0.0|
            +--------+---+----+
            <BLANKLINE>
        """
        return neglog_p * (r**2)

    @classmethod
    def finemap(
        cls: type[PICS], associations: StudyLocus, k: float = 6.4
    ) -> StudyLocus:
        """Run PICS on a study locus.

        !!! info "Study locus needs to be LD annotated"
            The study locus needs to be LD annotated before PICS can be calculated.

        Args:
            associations (StudyLocus): Study locus to finemap using PICS
            k (float): Empiric constant that can be adjusted to fit the curve, 6.4 recommended.

        Returns:
            StudyLocus: Study locus with PICS results
        """
        associations.df = (
            associations.df.withColumn("neglog_pvalue", associations.neglog_pvalue())
            .withColumn(
                "credibleSet",
                f.when(
                    f.col("credibleSet").isNotNull(),
                    f.transform(
                        f.col("credibleSet"),
                        lambda x: x.withField(
                            "posteriorProbability",
                            PICS._pics_posterior_probability(
                                f.col("studyLocusId"),
                                f.col("neglog_pvalue"),
                                x.r2Overall,
                                k,
                            ),
                        ),
                    ),
                ),
            )
            .drop("neglog_pvalue")
        )
        return associations
