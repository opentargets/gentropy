"""Calculate PICS for a given study and locus."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
from scipy.stats import norm

if TYPE_CHECKING:
    from otg.dataset.study_locus import StudyLocus


class PICS:
    """Probabilistic Identification of Causal SNPs (PICS), an algorithm estimating the probability that an individual variant is causal considering the haplotype structure and observed pattern of association at the genetic locus."""

    @staticmethod
    def _pics_relative_posterior_probability(
        neglog_p: float, pics_snp_mu: float, pics_snp_std: float
    ) -> float:
        """Compute the PICS posterior probability for a given SNP.

        !!! info "This probability needs to be scaled to take into account the probabilities of the other variants in the locus."

        Args:
            neglog_p (float): Negative log p-value of the lead variant
            pics_snp_mu (float): Mean P value of the association between a SNP and a trait
            pics_snp_std (float): Standard deviation for the P value of the association between a SNP and a trait

        Returns:
            Relative posterior probability of a SNP being causal in a locus

        Examples:
            >>> rel_prob = PICS._pics_relative_posterior_probability(neglog_p=10.0, pics_snp_mu=1.0, pics_snp_std=10.0)
            >>> round(rel_prob, 3)
            0.368
        """
        return float(norm(pics_snp_mu, pics_snp_std).sf(neglog_p) * 2)

    @staticmethod
    def _pics_standard_deviation(neglog_p: float, r2: float, k: float) -> float | None:
        """Compute the PICS standard deviation.

        This distribution is obtained after a series of permutation tests described in the PICS method, and it is only
        valid when the SNP is highly linked with the lead (r2 > 0.5).

        Args:
            neglog_p (float): Negative log p-value of the lead variant
            r2 (float): LD score between a given SNP and the lead variant
            k (float): Empiric constant that can be adjusted to fit the curve, 6.4 recommended.

        Returns:
            Standard deviation for the P value of the association between a SNP and a trait

        Examples:
            >>> PICS._pics_standard_deviation(neglog_p=1.0, r2=1.0, k=6.4)
            0.0
            >>> round(PICS._pics_standard_deviation(neglog_p=10.0, r2=0.5, k=6.4), 3)
            0.143
            >>> print(PICS._pics_standard_deviation(neglog_p=1.0, r2=0.0, k=6.4))
            None
        """
        return (
            (1 - abs(r2) ** 0.5**k) ** 0.5 * (neglog_p) ** 0.5 / 2
            if r2 >= 0.5
            else None
        )

    @staticmethod
    def _pics_mu(neglog_p: float, r2: float) -> float | None:
        """Compute the PICS mu that estimates the probability of association between a given SNP and the trait.

        This distribution is obtained after a series of permutation tests described in the PICS method, and it is only
        valid when the SNP is highly linked with the lead (r2 > 0.5).

        Args:
            neglog_p (float): Negative log p-value of the lead variant
            r2 (float): LD score between a given SNP and the lead variant

        Returns:
            Mean P value of the association between a SNP and a trait

        Examples:
            >>> PICS._pics_mu(neglog_p=1.0, r2=1.0)
            1.0
            >>> PICS._pics_mu(neglog_p=10.0, r2=0.5)
            5.0
            >>> print(PICS._pics_mu(neglog_p=10.0, r2=0.3))
            None
        """
        return neglog_p * r2 if r2 >= 0.5 else None

    @staticmethod
    def _finemap(credible_set: list, lead_neglog_p: float) -> list:
        """Calculates the probability of a variant being causal in a study-locus context by applying the PICS method.

        It is intended to be applied as an UDF in `PICS.finemap`, where each row is a StudyLocus association.
        The function iterates over every SNP in the `credibleSet` array, and it returns an updated credibleSet with
        its association signal and causality probability as of PICS.

        Args:
            credible_set (list): list of tagging variants after expanding the locus
            lead_neglog_p (float): P value of the association signal between the lead variant and the study in the form of -log10.

        Returns:
            List of tagging variants with an estimation of the association signal and their posterior probability as of PICS.
        """
        tmp_credible_set = []
        # First iteration: calculation of mu, standard deviation, and the relative posterior probability
        for tag_struct in credible_set:
            tag_dict = (
                tag_struct.asDict()
            )  # tag_struct is of type pyspark.Row, we'll represent it as a dict
            pics_snp_mu = PICS._pics_mu(lead_neglog_p, tag_dict["r2Overall"])
            pics_snp_std = PICS._pics_standard_deviation(
                lead_neglog_p, tag_dict["r2Overall"], 6.4
            )
            posterior_probability = (
                PICS._pics_relative_posterior_probability(
                    lead_neglog_p, pics_snp_mu, pics_snp_std
                )
                if pics_snp_mu and pics_snp_std
                else None
            )
            tag_dict["tagPValue"] = 10**-pics_snp_mu if pics_snp_mu else None
            tag_dict["tagStandardError"] = 10**-pics_snp_std if pics_snp_std else None
            tag_dict["relativePosteriorProbability"] = posterior_probability

            if posterior_probability is not None:
                tmp_credible_set.append(tag_dict)

        # Second iteration: calculation of the sum of all the posteriors in each study-locus, so that we scale them between 0-1
        total_posteriors = sum(
            tag_dict["relativePosteriorProbability"]
            for tag_dict in tmp_credible_set
            if tag_dict["relativePosteriorProbability"] is not None
        )

        # Third iteration: calculation of the final posteriorProbability
        new_credible_set = []
        for tag_dict in tmp_credible_set:
            tag_dict["posteriorProbability"] = float(
                tag_dict["relativePosteriorProbability"] / total_posteriors
            )
            tag_dict.pop("relativePosteriorProbability")
            new_credible_set.append(tag_dict)
        return new_credible_set

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
        # Register UDF by defining the structure of the output credibleSet array of structs
        credset_schema = t.ArrayType(
            associations.df.select("credibleSet").schema.fields[0].dataType.elementType  # type: ignore
        )
        _finemap_udf = f.udf(
            lambda credible_set, neglog_p: PICS._finemap(credible_set, neglog_p),
            credset_schema,
        )

        associations.df = (
            associations.df.withColumn("neglog_pvalue", associations.neglog_pvalue())
            .withColumn(
                "credibleSet",
                f.when(
                    f.col("credibleSet").isNotNull(),
                    _finemap_udf(f.col("credibleSet"), f.col("neglog_pvalue")),
                ),
            )
            .drop("neglog_pvalue")
        )
        return associations
