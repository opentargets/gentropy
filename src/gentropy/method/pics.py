"""Calculate PICS for a given study and locus."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
import pyspark.sql.types as t
from scipy.stats import norm

from gentropy.dataset.study_locus import StudyLocus, StudyLocusQualityCheck

if TYPE_CHECKING:
    from pyspark.sql import Row


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
            float: Posterior probability of the association between a SNP and a trait

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
            float | None: Standard deviation for the P value of the association between a SNP and a trait

        Examples:
            >>> PICS._pics_standard_deviation(neglog_p=1.0, r2=1.0, k=6.4)
            0.0
            >>> round(PICS._pics_standard_deviation(neglog_p=10.0, r2=0.5, k=6.4), 3)
            1.493
            >>> print(PICS._pics_standard_deviation(neglog_p=1.0, r2=0.0, k=6.4))
            None
        """
        return (
            abs(((1 - (r2**0.5) ** k) ** 0.5) * (neglog_p**0.5) / 2)
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
            float | None: Mean P value of the association between a SNP and a trait

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
    def _finemap(
        ld_set: list[Row], lead_neglog_p: float, k: float
    ) -> list[dict[str, Any]] | None:
        """Calculates the probability of a variant being causal in a study-locus context by applying the PICS method.

        It is intended to be applied as an UDF in `PICS.finemap`, where each row is a StudyLocus association.
        The function iterates over every SNP in the `ldSet` array, and it returns an updated locus with
        its association signal and causality probability as of PICS.

        Args:
            ld_set (list[Row]): list of tagging variants after expanding the locus
            lead_neglog_p (float): P value of the association signal between the lead variant and the study in the form of -log10.
            k (float): Empiric constant that can be adjusted to fit the curve, 6.4 recommended.

        Returns:
            list[dict[str, Any]] | None: List of tagging variants with an estimation of the association signal and their posterior probability as of PICS.

        Examples:
            >>> from pyspark.sql import Row
            >>> ld_set = [
            ...     Row(variantId="var1", r2Overall=0.8),
            ...     Row(variantId="var2", r2Overall=1),
            ... ]
            >>> PICS._finemap(ld_set, lead_neglog_p=10.0, k=6.4)
            [{'variantId': 'var1', 'r2Overall': 0.8, 'standardError': 0.07420896512708416, 'posteriorProbability': 0.07116959886882368}, {'variantId': 'var2', 'r2Overall': 1, 'standardError': 0.9977000638225533, 'posteriorProbability': 0.9288304011311763}]
            >>> empty_ld_set = []
            >>> PICS._finemap(empty_ld_set, lead_neglog_p=10.0, k=6.4)
            []
            >>> ld_set_with_no_r2 = [
            ...     Row(variantId="var1", r2Overall=None),
            ...     Row(variantId="var2", r2Overall=None),
            ... ]
            >>> PICS._finemap(ld_set_with_no_r2, lead_neglog_p=10.0, k=6.4)
            []
        """
        if ld_set is None:
            return None
        elif not ld_set:
            return []
        tmp_credible_set = []
        new_credible_set = []
        # First iteration: calculation of mu, standard deviation, and the relative posterior probability
        for tag_struct in ld_set:
            tag_dict = (
                tag_struct.asDict()
            )  # tag_struct is of type pyspark.Row, we'll represent it as a dict
            if (
                not tag_dict["r2Overall"]
                or tag_dict["r2Overall"] < 0.5
                or not lead_neglog_p
            ):
                # If PICS cannot be calculated, we drop the variant from the credible set
                continue

            pics_snp_mu = PICS._pics_mu(lead_neglog_p, tag_dict["r2Overall"])
            pics_snp_std = PICS._pics_standard_deviation(
                lead_neglog_p, tag_dict["r2Overall"], k
            )
            pics_snp_std = 0.001 if pics_snp_std == 0 else pics_snp_std
            if pics_snp_mu is not None and pics_snp_std is not None:
                posterior_probability = PICS._pics_relative_posterior_probability(
                    lead_neglog_p, pics_snp_mu, pics_snp_std
                )
                tag_dict["standardError"] = 10**-pics_snp_std
                tag_dict["relativePosteriorProbability"] = posterior_probability

                tmp_credible_set.append(tag_dict)

        # Second iteration: calculation of the sum of all the posteriors in each study-locus, so that we scale them between 0-1
        total_posteriors = sum(
            tag_dict.get("relativePosteriorProbability", 0)
            for tag_dict in tmp_credible_set
        )

        # Third iteration: calculation of the final posteriorProbability
        for tag_dict in tmp_credible_set:
            if total_posteriors != 0:
                tag_dict["posteriorProbability"] = float(
                    tag_dict.get("relativePosteriorProbability", 0) / total_posteriors
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
        # Register UDF by defining the structure of the output locus array of structs
        # it also renames tagVariantId to variantId

        picsed_ldset_schema = t.ArrayType(
            t.StructType(
                [
                    t.StructField("tagVariantId", t.StringType(), True),
                    t.StructField("r2Overall", t.DoubleType(), True),
                    t.StructField("posteriorProbability", t.DoubleType(), True),
                    t.StructField("standardError", t.DoubleType(), True),
                ]
            )
        )
        picsed_study_locus_schema = t.ArrayType(
            t.StructType(
                [
                    t.StructField("variantId", t.StringType(), True),
                    t.StructField("r2Overall", t.DoubleType(), True),
                    t.StructField("posteriorProbability", t.DoubleType(), True),
                    t.StructField("standardError", t.DoubleType(), True),
                ]
            )
        )
        _finemap_udf = f.udf(
            lambda locus, neglog_p: PICS._finemap(locus, neglog_p, k),
            picsed_ldset_schema,
        )
        non_picsable_expr = (
            f.size(f.filter(f.col("ldSet"), lambda x: x.r2Overall >= 0.5)) == 0
        )
        return StudyLocus(
            _df=(
                associations.df
                # Old locus column will be dropped if available
                .select(*[col for col in associations.df.columns if col != "locus"])
                # Estimate neglog_pvalue for the lead variant
                .withColumn("neglog_pvalue", associations.neglog_pvalue())
                # New locus containing the PICS results
                .withColumn(
                    "locus",
                    f.when(
                        f.col("ldSet").isNotNull(),
                        _finemap_udf(f.col("ldSet"), f.col("neglog_pvalue")).cast(
                            picsed_study_locus_schema
                        ),
                    ),
                )
                .withColumn(
                    "qualityControls",
                    StudyLocus.update_quality_flag(
                        f.col("qualityControls"),
                        non_picsable_expr,
                        StudyLocusQualityCheck.NOT_QUALIFYING_LD_BLOCK,
                    ),
                )
                .withColumn(
                    "finemappingMethod",
                    f.coalesce(f.col("finemappingMethod"), f.lit("pics")),
                )
                .withColumn(
                    "studyLocusId",
                    StudyLocus.assign_study_locus_id(
                        "studyId", "variantId", "finemappingMethod"
                    ),
                )
                .drop("neglog_pvalue")
            ),
            _schema=StudyLocus.get_schema(),
        )
