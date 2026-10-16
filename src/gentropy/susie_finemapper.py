"""Step to run a finemapping using SuSiE-inf.

Each run fine-maps one study locus into credible sets and their variants.

Conditional statistics are on the per-allele scale. SuSiE-inf sees only z-scores and the
sample size n (`X'y = sqrt(n)·z`, `X'X = n·LD`), so its posterior effect `mu` is in standard
deviation units, roughly `z / sqrt(n)`. For effect `l` with per-effect inclusion probabilities
`alpha_l` (`PIP[:, l]` of SuSiE-inf, which is SuSiE's alpha and not a per-variant PIP), the
alpha-weighted moments `alpha_l·mu_l` and `sqrt(alpha_l·(mu_l² + 1/omega_l) - (alpha_l·mu_l)²)`
are the quantities FinnGen reports as `mean{l}` and `sd{l}`, but FinnGen's are per allele. Both
are multiplied by `se_j·sqrt(n)`, with `se_j` the variant's marginal standard error and n the
sample size given to SuSiE-inf: since `z / sqrt(n) · se·sqrt(n) = z·se = beta`, this puts them
on the scale of the reported beta, so `conditionalBeta` means the same in every source. A
member without a marginal standard error (an imputed variant) has null conditional statistics.

Marginal statistics are the study's own, taken from the locus; none is recomputed from z,
which may be imputed. A member missing from the locus gets the `NOT_IN_LOCUS` flag, and the
lead p-value threshold drops a credible set whose lead has no reported p-value.

`r2Overall` is the squared correlation with the lead in the LD matrix used for fine-mapping,
an out-of-sample reference panel; every credible set carries the `OUT_OF_SAMPLE_LD` flag.

Study gating (study type, ancestry, summary statistics availability and study flags) is not done
here: the fine-mapping manifest decides which loci are fine-mapped.
"""

from __future__ import annotations

import logging
import math
import time
from typing import Any

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Row

from gentropy.common.ld import major_population
from gentropy.common.session import Session
from gentropy.dataset.credible_set import (
    CredibleSet,
    CredibleSetVariant,
    CredibleSetVariantQualityCheck,
)
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import (
    AnnotatedStudyLocus,
    FinemappingMethod,
    StudyLocusQualityCheck,
)
from gentropy.method.carma import CARMA
from gentropy.method.ld_matrix_interface import LDMatrixInterface
from gentropy.method.sumstat_imputation import SummaryStatisticsImputation
from gentropy.method.susie_inf import SUSIE_inf

# Credible set coverage: the cumulative posterior probability of its members.
COVERAGE = 0.95

# Sample size given to SuSiE-inf when the study index reports none.
DEFAULT_SAMPLE_SIZE = 100_000

# Fields of a locus entry carried to the credible set variants.
MARGINAL_COLUMNS = (
    "variantId",
    "pValueMantissa",
    "pValueExponent",
    "effectAlleleFrequencyFromSource",
    "sampleSize",
    "beta",
    "standardError",
)

_MEMBERS_SCHEMA = (
    "credibleSetIndex int, variantId string, posteriorProbability double, "
    "logBayesFactor double, conditionalBeta double, conditionalStandardError double, "
    "r2Overall double, pValueMantissa float, pValueExponent int, "
    "effectAlleleFrequency float, sampleSize int, reportedBeta double, "
    "reportedStandardError double, inLocus boolean, studyId string, studyType string, "
    "chromosome string, locusStart int, locusEnd int, fineMappingMethod string, "
    "credibleSetLogBayesFactor double, purityMeanR2 double, purityMinR2 double"
)


class SusieFineMapperStep:
    """SuSie finemaping. It has generic methods to run SuSie fine mapping for a study locus.

    This class/step is the temporary solution of the fine-mapping warpper for the development purposes.
    In the future this step will be refactored and moved to the methods module.
    """

    def __init__(
        self,
        session: Session,
        study_index_path: str,
        study_locus_manifest_path: str,
        study_locus_index: int,
        ld_matrix_paths: dict[str, str],
        max_causal_snps: int = 10,
        lead_pval_threshold: float = 1e-5,
        purity_mean_r2_threshold: float = 0,
        purity_min_r2_threshold: float = 0.25,
        cs_lbf_thr: float = 2,
        susie_est_tausq: bool = False,
        run_carma: bool = False,
        run_sumstat_imputation: bool = False,
        carma_time_limit: int = 600,
        carma_tau: float = 0.15,
        imputed_r2_threshold: float = 0.9,
        ld_score_threshold: float = 5,
        ld_min_r2: float = 0.8,
    ) -> None:
        """Run fine-mapping on one study locus listed in a manifest.

        The `study_locus_manifest_path` CSV has the columns `study_locus_input` (an
        AnnotatedStudyLocus with one locus), `credible_set_output` and
        `credible_set_variant_output`, and optionally `log_output`. Without `log_output`, the
        log is written next to `credible_set_output` with a `.log` suffix. Each execution
        evaluates the single manifest row at `study_locus_index`.

        Args:
            session (Session): Spark session
            study_index_path (str): path to the study index
            study_locus_manifest_path (str): Path to the CSV manifest with the input and output locations
            study_locus_index (int): Index (0-based) of the locus in the manifest to process in this call
            ld_matrix_paths (dict[str, str]): Dictionary with paths to LD matrices
            max_causal_snps (int): Maximum number of causal variants in locus, default is 10
            lead_pval_threshold (float): p-value threshold for the lead variant from CS, default is 1e-5
            purity_mean_r2_threshold (float): threshold for purity mean r2 qc metrics for filtering credible sets, default is 0
            purity_min_r2_threshold (float): threshold for purity min r2 qc metrics for filtering credible sets, default is 0.25
            cs_lbf_thr (float): credible set logBF threshold for filtering credible sets, default is 2
            susie_est_tausq (bool): estimate tau squared, default is False
            run_carma (bool): run CARMA, default is False
            run_sumstat_imputation (bool): run summary statistics imputation, default is False
            carma_time_limit (int): CARMA time limit, default is 600 seconds
            carma_tau (float): CARMA tau, shrinkage parameter
            imputed_r2_threshold (float): imputed R2 threshold, default is 0.9
            ld_score_threshold (float): LD score threshold ofr imputation, default is 5
            ld_min_r2 (float): Threshold to filter CS by leads in high LD, default is 0.8
        """
        # Read locus manifest.
        study_locus_manifest = pd.read_csv(study_locus_manifest_path)
        row = study_locus_manifest.loc[study_locus_index]
        study_locus_input = row["study_locus_input"]
        credible_set_output = row["credible_set_output"]
        credible_set_variant_output = row["credible_set_variant_output"]
        log_output = credible_set_output + ".log"
        if "log_output" in study_locus_manifest.columns:
            log_output = row["log_output"] + ".log"

        study_locus = AnnotatedStudyLocus.from_parquet(
            session, study_locus_input
        ).df.collect()[0]
        study_index = StudyIndex.from_parquet(session, study_index_path)
        # Run fine-mapping

        result_logging = self.susie_finemapper_one_sl_row_gathered_boundaries(
            session=session,
            study_locus_row=study_locus,
            study_index=study_index,
            ld_matrix_paths=ld_matrix_paths,
            max_causal_snps=max_causal_snps,
            purity_mean_r2_threshold=purity_mean_r2_threshold,
            purity_min_r2_threshold=purity_min_r2_threshold,
            cs_lbf_thr=cs_lbf_thr,
            lead_pval_threshold=lead_pval_threshold,
            susie_est_tausq=susie_est_tausq,
            run_carma=run_carma,
            run_sumstat_imputation=run_sumstat_imputation,
            carma_tau=carma_tau,
            carma_time_limit=carma_time_limit,
            imputed_r2_threshold=imputed_r2_threshold,
            ld_score_threshold=ld_score_threshold,
            ld_min_r2=ld_min_r2,
            log_output=log_output,
        )

        if result_logging is not None:
            if result_logging["credible_set"] is not None:
                # Write result
                for dataset, path in (
                    (result_logging["credible_set"], credible_set_output),
                    (
                        result_logging["credible_set_variant"],
                        credible_set_variant_output,
                    ),
                ):
                    dataset.df.coalesce(session.output_partitions).write.mode(
                        session.write_mode
                    ).parquet(path)
            if result_logging["log"] is not None:
                # Write log
                result_logging["log"].to_csv(log_output, index=False, sep="\t")

    @staticmethod
    def _empty_log_mg(studyId: str, region: str, error_mg: str, path_out: str) -> None:
        """Create an empty log DataFrame with error message.

        Args:
            studyId (str): study ID
            region (str): region
            error_mg (str): error message
            path_out (str): output path
        """
        pd.DataFrame(
            {
                "studyId": studyId,
                "region": region,
                "N_gwas_before_dedupl": 0,
                "N_gwas": 0,
                "N_ld": 0,
                "N_overlap": 0,
                "N_outliers": 0,
                "N_imputed": 0,
                "N_final_to_fm": 0,
                "elapsed_time": 0,
                "number_of_CS": 0,
                "error": error_mg,
            },
            index=[0],
        ).to_csv(path_out, index=False, sep="\t")

    @staticmethod
    def susie_inf_to_credible_sets(  # noqa: C901
        susie_output: dict[str, Any],
        session: Session,
        studyId: str,
        variants: pd.DataFrame,
        ld_matrix: np.ndarray,
        locusStart: int,
        locusEnd: int,
        n: int,
        marginals: DataFrame | None = None,
        study_type: str = "gwas",
        quality_controls: list[str] | None = None,
        cs_lbf_thr: float = 2,
        lead_pval_threshold: float | None = 1,
        purity_mean_r2_threshold: float = 0,
        purity_min_r2_threshold: float = 0,
        ld_min_r2: float = 0.9,
    ) -> tuple[CredibleSet, CredibleSetVariant] | None:
        """Convert SuSiE-inf output to credible sets and their variants.

        Effects are taken in decreasing order of their Bayes factor: the first always, the others
        when it reaches `cs_lbf_thr`. A credible set holds the variants with the highest `alpha_l`
        up to a cumulative 95%, and is led by its highest `alpha_l`, ties broken by `variantId`.
        Credible sets are then filtered, in order, on the lead p-value, the Bayes factor and the
        purity; one credible set is kept per lead, and of leads with r2 of at least `ld_min_r2`
        only the one with the highest |z| is kept.

        Statistics, as described in the module docstring:

        - `posteriorProbability` is `alpha_l` and `logBayesFactor` is `lbf_variable_l`;
        - `conditionalBeta` and `conditionalStandardError` are the alpha-weighted moments of
          effect l, multiplied by `se_j·sqrt(n)` to the per-allele scale;
        - `r2Overall` is the squared LD with the lead in `ld_matrix`;
        - the marginal statistics are the member's entry in `marginals`;
        - the credible set `logBayesFactor` is SuSiE-inf's `lbf_l`, which is already
          over every variant of the region;
        - credible set flags are `quality_controls` plus `OUT_OF_SAMPLE_LD`.

        Args:
            susie_output (dict[str, Any]): SuSiE-inf output dictionary
            session (Session): Spark session
            studyId (str): study ID
            variants (pd.DataFrame): The fine-mapped variants, in the order of `ld_matrix`, with
                `variantId` and `z`
            ld_matrix (np.ndarray): LD matrix used for fine-mapping
            locusStart (int): locus start
            locusEnd (int): locus end
            n (int): sample size given to SuSiE-inf
            marginals (DataFrame | None): The study's statistics per variant, as `MARGINAL_COLUMNS`;
                None leaves every member without marginal statistics
            study_type (str): study type of the credible sets, default is "gwas"
            quality_controls (list[str] | None): flags of the fine-mapped study locus
            cs_lbf_thr (float): credible set logBF threshold for filtering credible sets, default is 2
            lead_pval_threshold (float | None): p-value threshold for the lead variant from CS;
                None keeps credible sets whatever their lead p-value
            purity_mean_r2_threshold (float): thrshold for purity mean r2 qc metrics for filtering credible sets
            purity_min_r2_threshold (float): thrshold for purity min r2 qc metrics for filtering credible sets
            ld_min_r2 (float): Threshold to fillter CS by leads in high LD, default is 0.9

        Returns:
            tuple[CredibleSet, CredibleSetVariant] | None: credible sets and their variants, or
                None when no credible set passes the filters
        """
        variant_ids = variants["variantId"].to_numpy()
        z_values = variants["z"].to_numpy(dtype=float)
        alphas = susie_output["PIP"]
        lbfs = susie_output["lbf_variable"]
        mu = susie_output["mu"]
        omega = susie_output["omega"]
        study_marginals = (
            {}
            if marginals is None
            else {
                row["variantId"]: row.asDict()
                for row in marginals.select(*MARGINAL_COLUMNS).collect()
            }
        )

        def lead_neglogpval(lead: str) -> float | None:
            marginal = study_marginals.get(lead)
            if marginal is None or marginal["pValueMantissa"] is None:
                return None
            return -(
                math.log10(marginal["pValueMantissa"]) + marginal["pValueExponent"]
            )

        # Extracting credible sets
        order_creds = list(enumerate(susie_output["lbf"]))
        order_creds.sort(key=lambda x: x[1], reverse=True)

        cred_sets = []
        for counter, (i, cs_lbf_value) in enumerate(order_creds, start=1):
            if counter > 1 and cs_lbf_value < cs_lbf_thr:
                continue
            alpha = alphas[:, i]
            ranked = np.lexsort((variant_ids, -alpha))
            size = min(
                int(np.searchsorted(np.cumsum(alpha[ranked]), COVERAGE)) + 1,
                len(ranked),
            )
            members = ranked[:size]
            squared_matrix = ld_matrix[members, :][:, members] ** 2
            cred_sets.append(
                {
                    "credibleSetIndex": counter,
                    "effect": i,
                    "members": members,
                    "lead": members[0],
                    "leadVariantId": variant_ids[members[0]],
                    "neglogpval": lead_neglogpval(variant_ids[members[0]]),
                    "credibleSetLogBayesFactor": cs_lbf_value,
                    "purityMeanR2": float(np.mean(squared_matrix)),
                    "purityMinR2": float(np.min(squared_matrix)),
                }
            )

        # Filter by lead p-value, credible set logBF, purity mean r2 and purity min r2
        cred_sets = [
            cs
            for cs in cred_sets
            if (
                lead_pval_threshold is None
                or (
                    cs["neglogpval"] is not None
                    and cs["neglogpval"] >= -np.log10(lead_pval_threshold)
                )
            )
            and cs["credibleSetLogBayesFactor"] >= cs_lbf_thr
            and not np.isnan(cs["credibleSetLogBayesFactor"])
            and cs["purityMinR2"] >= purity_min_r2_threshold
            and cs["purityMeanR2"] >= purity_mean_r2_threshold
        ]
        if not cred_sets:
            return None

        # Remove duplicated by lead variant
        first_by_lead: dict[str, dict[str, Any]] = {}
        for cs in sorted(cred_sets, key=lambda cs: cs["credibleSetIndex"]):
            first_by_lead.setdefault(cs["leadVariantId"], cs)
        cred_sets = list(first_by_lead.values())

        # Remove CSs with high LD between leads
        if len(cred_sets) > 1:
            cred_sets.sort(key=lambda cs: abs(z_values[cs["lead"]]), reverse=True)
            ind = [cs["lead"] for cs in cred_sets]
            ld_leads = ld_matrix[ind, :][:, ind]
            ld_leads = ld_leads**2
            ld_leads = ld_leads - np.tril(ld_leads)
            np.fill_diagonal(ld_leads, -1)

            to_delete: set[int] = set()
            for idx in range(len(cred_sets)):
                if idx in to_delete:
                    continue
                to_delete.update(np.where(ld_leads[idx, :] >= ld_min_r2)[0].tolist())
            cred_sets = [cs for idx, cs in enumerate(cred_sets) if idx not in to_delete]

        rows = []
        for cs in cred_sets:
            effect, lead = cs["effect"], cs["lead"]
            for j in cs["members"]:
                marginal = study_marginals.get(variant_ids[j])
                alpha_mu = alphas[j, effect] * mu[j, effect]
                variance = (
                    alphas[j, effect] * (mu[j, effect] ** 2 + 1 / omega[j, effect])
                    - alpha_mu**2
                )
                se = None if marginal is None else marginal["standardError"]
                scale = None if se is None else se * math.sqrt(n)
                rows.append(
                    (
                        cs["credibleSetIndex"],
                        str(variant_ids[j]),
                        float(alphas[j, effect]),
                        float(lbfs[j, effect]),
                        None if scale is None else float(alpha_mu * scale),
                        None
                        if scale is None
                        else float(math.sqrt(max(variance, 0)) * scale),
                        float(ld_matrix[lead, j] ** 2),
                        *(
                            (None,) * 6
                            if marginal is None
                            else (
                                marginal["pValueMantissa"],
                                marginal["pValueExponent"],
                                marginal["effectAlleleFrequencyFromSource"],
                                marginal["sampleSize"],
                                marginal["beta"],
                                se,
                            )
                        ),
                        marginal is not None,
                        studyId,
                        study_type,
                        str(cs["leadVariantId"]).split("_")[0],
                        int(locusStart),
                        int(locusEnd),
                        FinemappingMethod.SUSIE_INF.value,
                        float(cs["credibleSetLogBayesFactor"]),
                        cs["purityMeanR2"],
                        cs["purityMinR2"],
                    )
                )

        # Literal arrays, as the schemas allow no null flag:
        members_df = session.spark.createDataFrame(rows, _MEMBERS_SCHEMA).withColumns(
            {
                "credibleSetQualityControls": f.array(
                    *(f.lit(flag) for flag in quality_controls or [])
                ),
                "qualityControls": f.when(
                    ~f.col("inLocus"),
                    f.array(f.lit(CredibleSetVariantQualityCheck.NOT_IN_LOCUS.value)),
                )
                .when(
                    f.col("effectAlleleFrequency").isNotNull(),
                    f.array(
                        f.lit(CredibleSetVariantQualityCheck.EAF_FROM_SOURCE.value)
                    ),
                )
                .otherwise(f.array()),
            }
        )
        credible_sets, credible_set_variants = CredibleSet.from_members(
            members_df, ["credibleSetIndex"]
        )
        credible_sets.df = credible_sets.df.withColumn(
            "qualityControls",
            CredibleSet.update_quality_flag(
                f.col("qualityControls"),
                f.lit(True),
                StudyLocusQualityCheck.OUT_OF_SAMPLE_LD,
            ),
        )
        return credible_sets, credible_set_variants

    @staticmethod
    def susie_finemapper_from_prepared_dataframes(
        GWAS_df: DataFrame,
        ld_index: DataFrame,
        gnomad_ld: np.ndarray,
        L: int,
        session: Session,
        studyId: str,
        region: str,
        locusStart: int,
        locusEnd: int,
        marginals: DataFrame | None = None,
        study_type: str = "gwas",
        quality_controls: list[str] | None = None,
        susie_est_tausq: bool = False,
        run_carma: bool = False,
        run_sumstat_imputation: bool = False,
        carma_time_limit: int = 600,
        carma_tau: float = 0.04,
        imputed_r2_threshold: float = 0.8,
        ld_score_threshold: float = 4,
        lead_pval_threshold: float | None = 1e-5,
        purity_mean_r2_threshold: float = 0,
        purity_min_r2_threshold: float = 0.25,
        cs_lbf_thr: float = 2,
        ld_min_r2: float = 0.9,
        N_total: int = DEFAULT_SAMPLE_SIZE,
    ) -> dict[str, Any] | None:
        """Susie fine-mapper function that uses LD, z-scores, variant info and other options for Fine-Mapping.

        Args:
            GWAS_df (DataFrame): GWAS DataFrame with mandatary columns: z, variantId
            ld_index (DataFrame): LD index DataFrame
            gnomad_ld (np.ndarray): GnomAD LD matrix
            L (int): number of causal variants
            session (Session): Spark session
            studyId (str): study ID
            region (str): region
            locusStart (int): locus start
            locusEnd (int): locus end
            marginals (DataFrame | None): The study's statistics per variant, as `MARGINAL_COLUMNS`
            study_type (str): study type of the credible sets, default is "gwas"
            quality_controls (list[str] | None): flags of the fine-mapped study locus
            susie_est_tausq (bool): estimate tau squared, default is False
            run_carma (bool): run CARMA, default is False
            run_sumstat_imputation (bool): run summary statistics imputation, default is False
            carma_time_limit (int): CARMA time limit, default is 600 seconds
            carma_tau (float): CARMA tau, shrinkage parameter
            imputed_r2_threshold (float): imputed R2 threshold, default is 0.8
            ld_score_threshold (float): LD score threshold ofr imputation, default is 4
            lead_pval_threshold (float | None): p-value threshold for the lead variant from CS, default is 1e-5
            purity_mean_r2_threshold (float): thrshold for purity mean r2 qc metrics for filtering credible sets
            purity_min_r2_threshold (float): thrshold for purity min r2 qc metrics for filtering credible sets
            cs_lbf_thr (float): credible set logBF threshold for filtering credible sets, default is 2
            ld_min_r2 (float): Threshold to fillter CS by leads in high LD, default is 0.9
            N_total (int): total number of samples, default is 100_000

        Returns:
            dict[str, Any] | None: dictionary with the credible sets, their variants, the log, the LD matrix and the fine-mapped variants
        """
        # PLEASE DO NOT REMOVE THIS LINE
        pd.DataFrame.iteritems = pd.DataFrame.items

        start_time = time.time()
        GWAS_df = GWAS_df.toPandas()
        N_gwas_before_dedupl = len(GWAS_df)

        GWAS_df = GWAS_df.drop_duplicates(subset="variantId", keep=False)
        GWAS_df = GWAS_df.reset_index()

        ld_index = ld_index.toPandas()
        ld_index = ld_index.reset_index()

        N_gwas = len(GWAS_df)
        N_ld = len(ld_index)

        # Filtering out the variants that are not in the LD matrix, we don't need them
        df_columns = ["variantId", "z"]
        GWAS_df = GWAS_df.merge(ld_index, on="variantId", how="inner")
        GWAS_df = GWAS_df[df_columns].reset_index()
        N_after_merge = len(GWAS_df)

        merged_df = GWAS_df.merge(
            ld_index, left_on="variantId", right_on="variantId", how="inner"
        )
        indices = merged_df["index_y"].values

        ld_to_fm = gnomad_ld[indices][:, indices]
        z_to_fm = GWAS_df["z"].values

        if run_carma:
            carma_output = CARMA.time_limited_CARMA_spike_slab_noEM(
                z=z_to_fm, ld=ld_to_fm, sec_threshold=carma_time_limit, tau=carma_tau
            )
            if carma_output["Outliers"] != [] and carma_output["Outliers"] is not None:
                GWAS_df.drop(carma_output["Outliers"], inplace=True)
                GWAS_df = GWAS_df.reset_index()
                ld_index = ld_index.reset_index()
                merged_df = GWAS_df.merge(
                    ld_index, left_on="variantId", right_on="variantId", how="inner"
                )
                indices = merged_df["index_y"].values

                ld_to_fm = gnomad_ld[indices][:, indices]
                z_to_fm = GWAS_df["z"].values
                N_outliers = len(carma_output["Outliers"])
            else:
                N_outliers = 0
        else:
            N_outliers = 0

        if run_sumstat_imputation:
            known = indices
            unknown = [
                index for index in list(range(len(gnomad_ld))) if index not in known
            ]
            sig_t = gnomad_ld[known, :][:, known]
            sig_i_t = gnomad_ld[unknown, :][:, known]
            zt = z_to_fm

            sumstat_imp_res = SummaryStatisticsImputation.raiss_model(
                z_scores_known=zt,
                ld_matrix_known=sig_t,
                ld_matrix_known_missing=sig_i_t,
                lamb=0.01,
                rtol=0.01,
            )

            bool_index = (sumstat_imp_res["imputation_r2"] >= imputed_r2_threshold) * (
                sumstat_imp_res["ld_score"] >= ld_score_threshold
            )
            if sum(bool_index) >= 1:
                indices = np.where(bool_index)[0]
                index_to_add = [unknown[i] for i in indices]
                index_to_fm = np.concatenate((known, index_to_add))

                ld_to_fm = gnomad_ld[index_to_fm][:, index_to_fm]

                snp_info_to_add = pd.DataFrame(
                    {
                        "variantId": ld_index.iloc[index_to_add, :]["variantId"],
                        "z": sumstat_imp_res["mu"][indices],
                    }
                )
                GWAS_df = pd.concat([GWAS_df, snp_info_to_add], ignore_index=True)
                z_to_fm = GWAS_df["z"].values

                N_imputed = len(indices)
            else:
                N_imputed = 0
        else:
            N_imputed = 0

        susie_output = SUSIE_inf.susie_inf(
            z=z_to_fm, LD=ld_to_fm, L=L, est_tausq=susie_est_tausq, n=N_total
        )

        credible_sets = SusieFineMapperStep.susie_inf_to_credible_sets(
            susie_output=susie_output,
            session=session,
            studyId=studyId,
            variants=GWAS_df[["variantId", "z"]],
            ld_matrix=ld_to_fm,
            locusStart=locusStart,
            locusEnd=locusEnd,
            n=N_total,
            marginals=marginals,
            study_type=study_type,
            quality_controls=quality_controls,
            lead_pval_threshold=lead_pval_threshold,
            purity_mean_r2_threshold=purity_mean_r2_threshold,
            purity_min_r2_threshold=purity_min_r2_threshold,
            cs_lbf_thr=cs_lbf_thr,
            ld_min_r2=ld_min_r2,
        )
        credible_set, credible_set_variant = credible_sets or (None, None)

        end_time = time.time()

        log_df = pd.DataFrame(
            {
                "studyId": studyId,
                "region": region,
                "N_gwas_before_dedupl": N_gwas_before_dedupl,
                "N_gwas": N_gwas,
                "N_ld": N_ld,
                "N_overlap": N_after_merge,
                "N_outliers": N_outliers,
                "N_imputed": N_imputed,
                "N_final_to_fm": len(ld_to_fm),
                "elapsed_time": end_time - start_time,
                "number_of_CS": 0 if credible_set is None else credible_set.df.count(),
                "error": "",
            },
            index=[0],
        )

        return {
            "credible_set": credible_set,
            "credible_set_variant": credible_set_variant,
            "log": log_df,
            "LD": ld_to_fm,
            "GWAS_df": GWAS_df,
        }

    @staticmethod
    def susie_finemapper_one_sl_row_gathered_boundaries(  # noqa: C901
        session: Session,
        study_locus_row: Row,
        study_index: StudyIndex,
        ld_matrix_paths: dict[str, str],
        max_causal_snps: int = 10,
        susie_est_tausq: bool = False,
        run_carma: bool = False,
        run_sumstat_imputation: bool = False,
        carma_time_limit: int = 600,
        carma_tau: float = 0.04,
        imputed_r2_threshold: float = 0.9,
        ld_score_threshold: float = 5,
        lead_pval_threshold: float = 1e-5,
        purity_mean_r2_threshold: float = 0,
        purity_min_r2_threshold: float = 0.25,
        cs_lbf_thr: float = 2,
        ld_min_r2: float = 0.9,
        log_output: str = "",
    ) -> dict[str, Any] | None:
        """Susie fine-mapper function that uses an annotated study-locus row as input.

        The study index gives the major population, which chooses the LD matrix, the study type
        and the sample size, 100,000 when the study reports none. Whether the study should be
        fine-mapped at all is decided by the fine-mapping manifest, not here.

        Args:
            session (Session): Spark session
            study_locus_row (Row): AnnotatedStudyLocus row with collected locus
            study_index (StudyIndex): StudyIndex object
            ld_matrix_paths (dict[str, str]): Dictionary with paths to LD matrices
            max_causal_snps (int): maximum number of causal variants
            susie_est_tausq (bool): estimate tau squared, default is False
            run_carma (bool): run CARMA, default is False
            run_sumstat_imputation (bool): run summary statistics imputation, default is False
            carma_time_limit (int): CARMA time limit, default is 600 seconds
            carma_tau (float): CARMA tau, shrinkage parameter
            imputed_r2_threshold (float): imputed R2 threshold, default is 0.8
            ld_score_threshold (float): LD score threshold ofr imputation, default is 4
            lead_pval_threshold (float): p-value threshold for the lead variant from CS, default is 1e-5
            purity_mean_r2_threshold (float): thrshold for purity mean r2 qc metrics for filtering credible sets
            purity_min_r2_threshold (float): thrshold for purity min r2 qc metrics for filtering credible sets
            cs_lbf_thr (float): credible set logBF threshold for filtering credible sets, default is 2
            ld_min_r2 (float): Threshold to fillter CS by leads in high LD, default is 0.9
            log_output (str): path to the log output

        Returns:
            dict[str, Any] | None: dictionary with the credible sets, their variants, the log, the LD matrix and the fine-mapped variants, or None
        """
        # PLEASE DO NOT REMOVE THIS LINE
        pd.DataFrame.iteritems = pd.DataFrame.items

        chromosome = study_locus_row["chromosome"]
        studyId = study_locus_row["studyId"]
        locusStart = study_locus_row["locusStart"]
        locusEnd = study_locus_row["locusEnd"]

        study = (
            study_index.df.filter(f.col("studyId") == studyId)
            .select(
                "studyType",
                "nSamples",
                major_population(f.col("ldPopulationStructure")).alias(
                    "majorPopulation"
                ),
            )
            .collect()
        )

        if not study:
            if log_output != "":
                SusieFineMapperStep._empty_log_mg(
                    studyId=studyId,
                    region="",
                    error_mg="No study index found for the studyId",
                    path_out=log_output,
                )
            logging.warning("No study index found for the studyId")
            return None

        major_population_name = study[0]["majorPopulation"]
        # This is a temporary solution
        if major_population_name == "eas":
            major_population_name = "csa"

        N_total = (
            DEFAULT_SAMPLE_SIZE
            if study[0]["nSamples"] is None
            else int(study[0]["nSamples"])
        )

        locusStart = max(locusStart, 0)

        region = chromosome + ":" + str(int(locusStart)) + "-" + str(int(locusEnd))

        if major_population_name is None:
            if log_output != "":
                SusieFineMapperStep._empty_log_mg(
                    studyId=studyId,
                    region=region,
                    error_mg="No LD population for the study",
                    path_out=log_output,
                )
            logging.warning("No LD population for the study")
            return None

        gwas_df = session.spark.createDataFrame(
            [study_locus_row], AnnotatedStudyLocus.get_schema()
        )
        exploded_df = gwas_df.select(f.explode("locus").alias("locus"))

        result_df = exploded_df.select(
            *(f"locus.{column}" for column in MARGINAL_COLUMNS)
        )
        gwas_df = (
            result_df.withColumn("z", f.col("beta") / f.col("standardError"))
            .withColumn(
                "chromosome", f.split(f.col("variantId"), "_")[0].cast("string")
            )
            .withColumn("position", f.split(f.col("variantId"), "_")[1].cast("int"))
            .filter(f.col("chromosome") == chromosome)
            .filter(f.col("position") >= int(locusStart))
            .filter(f.col("position") <= int(locusEnd))
            .filter(f.col("z").isNotNull())
        )

        # Remove ALL duplicated variants from GWAS DataFrame - we don't know which is correct
        variant_counts = gwas_df.groupBy("variantId").count()
        unique_variants = variant_counts.filter(f.col("count") == 1)
        gwas_df = gwas_df.join(unique_variants, on="variantId", how="left_semi")
        marginals = gwas_df.select(*MARGINAL_COLUMNS)

        ld_index = LDMatrixInterface.get_locus_index_boundaries(
            ld_matrix_paths=ld_matrix_paths,
            study_locus_row=study_locus_row,
            ancestry=major_population_name,
            session=session,
        )

        # Remove ALL duplicated variants from ld_index DataFrame - we don't know which is correct
        variant_counts = ld_index.groupBy("variantId").count()
        unique_variants = variant_counts.filter(f.col("count") == 1)
        ld_index = ld_index.join(unique_variants, on="variantId", how="left_semi").sort(
            "idx"
        )
        if "alleleOrder" not in ld_index.columns:
            ld_index = ld_index.withColumn("alleleOrder", f.lit(1))

        if not run_sumstat_imputation:
            # Filtering out the variants that are not in the LD matrix, we don't need them
            gwas_index = gwas_df.join(
                ld_index.select("variantId", "idx", "alleleOrder"), on="variantId"
            ).sort("idx")
            gwas_df = gwas_index.select(
                "variantId",
                "z",
                "chromosome",
                "position",
                "beta",
                "StandardError",
            )
            gwas_index = gwas_index.drop(
                "z", "chromosome", "position", "beta", "StandardError"
            )
            if gwas_index.rdd.isEmpty():
                if log_output != "":
                    SusieFineMapperStep._empty_log_mg(
                        studyId=studyId,
                        region=region,
                        error_mg="No overlapping variants in the LD Index",
                        path_out=log_output,
                    )
                logging.warning("No overlapping variants in the LD Index")
                return None
            gnomad_ld = LDMatrixInterface.get_numpy_matrix(
                ld_matrix_paths=ld_matrix_paths,
                locus_index=gwas_index,
                ancestry=major_population_name,
            )

            # Module to remove NANs from the LD matrix
            if sum(sum(np.isnan(gnomad_ld))) > 0:
                gwas_index = gwas_index.toPandas()

                # First round of filtering out the variants with NANs
                nan_count = 1 - (sum(np.isnan(gnomad_ld)) / len(gnomad_ld))
                indices = np.where(nan_count >= 0.98)
                indices = indices[0]
                gnomad_ld = gnomad_ld[indices][:, indices]

                gwas_index = gwas_index.iloc[indices, :]

                if len(gwas_index) == 0:
                    if log_output != "":
                        SusieFineMapperStep._empty_log_mg(
                            studyId=studyId,
                            region=region,
                            error_mg="No overlapping variants in the LD Index",
                            path_out=log_output,
                        )
                    logging.warning("No overlapping variants in the LD Index")
                    return None

                # Second round of filtering out the variants with NANs
                nan_count = sum(np.isnan(gnomad_ld))
                indices = np.where(nan_count == 0)
                indices = indices[0]

                gnomad_ld = gnomad_ld[indices][:, indices]
                gwas_index = gwas_index.iloc[indices, :]

                if len(gwas_index) == 0:
                    if log_output != "":
                        SusieFineMapperStep._empty_log_mg(
                            studyId=studyId,
                            region=region,
                            error_mg="No overlapping variants in the LD Index",
                            path_out=log_output,
                        )
                    logging.warning("No overlapping variants in the LD Index")
                    return None

                gwas_index = session.spark.createDataFrame(gwas_index)

        else:
            gwas_index = gwas_df.join(
                ld_index.select("variantId", "idx", "alleleOrder"), on="variantId"
            ).sort("idx")
            if gwas_index.rdd.isEmpty():
                if log_output != "":
                    SusieFineMapperStep._empty_log_mg(
                        studyId=studyId,
                        region=region,
                        error_mg="No overlapping variants in the LD Index",
                        path_out=log_output,
                    )
                logging.warning("No overlapping variants in the LD Index")
                return None
            gwas_index = ld_index
            gnomad_ld = LDMatrixInterface.get_numpy_matrix(
                ld_matrix_paths=ld_matrix_paths,
                locus_index=gwas_index,
                ancestry=major_population_name,
            )

            # Module to remove NANs from the LD matrix
            if sum(sum(np.isnan(gnomad_ld))) > 0:
                gwas_index = gwas_index.toPandas()

                # First round of filtering out the variants with NANs
                nan_count = 1 - (sum(np.isnan(gnomad_ld)) / len(gnomad_ld))
                indices = np.where(nan_count >= 0.98)
                indices = indices[0]
                gnomad_ld = gnomad_ld[indices][:, indices]

                gwas_index = gwas_index.iloc[indices, :]

                if len(gwas_index) == 0:
                    if log_output != "":
                        SusieFineMapperStep._empty_log_mg(
                            studyId=studyId,
                            region=region,
                            error_mg="No overlapping variants in the LD Index",
                            path_out=log_output,
                        )
                    logging.warning("No overlapping variants in the LD Index")
                    return None

                # Second round of filtering out the variants with NANs
                nan_count = sum(np.isnan(gnomad_ld))
                indices = np.where(nan_count == 0)
                indices = indices[0]

                gnomad_ld = gnomad_ld[indices][:, indices]
                gwas_index = gwas_index.iloc[indices, :]

                if len(gwas_index) == 0:
                    if log_output != "":
                        SusieFineMapperStep._empty_log_mg(
                            studyId=studyId,
                            region=region,
                            error_mg="No overlapping variants in the LD Index",
                            path_out=log_output,
                        )
                    logging.warning("No overlapping variants in the LD Index")
                    return None

                gwas_index = session.spark.createDataFrame(gwas_index)

        # sanity filters on LD matrix
        np.fill_diagonal(gnomad_ld, 1)
        gnomad_ld[gnomad_ld > 1] = 1
        gnomad_ld[gnomad_ld < -1] = -1
        upper_triangle = np.triu(gnomad_ld)
        gnomad_ld = (
            upper_triangle + upper_triangle.T - np.diag(upper_triangle.diagonal())
        )
        np.fill_diagonal(gnomad_ld, 1)

        # Desision tree - number of variants
        if gwas_index.count() < 100:
            if log_output != "":
                SusieFineMapperStep._empty_log_mg(
                    studyId=studyId,
                    region=region,
                    error_mg="Less than 100 variants after joining GWAS and LD index",
                    path_out=log_output,
                )
            logging.warning("Less than 100 variants after joining GWAS and LD index")
            return None
        elif gwas_index.count() >= 15_000:
            if log_output != "":
                SusieFineMapperStep._empty_log_mg(
                    studyId=studyId,
                    region=region,
                    error_mg="More than 15000 variants after joining GWAS and LD index",
                    path_out=log_output,
                )
            logging.warning("More than 15000 variants after joining GWAS and LD index")
            return None

        out = SusieFineMapperStep.susie_finemapper_from_prepared_dataframes(
            GWAS_df=gwas_df,
            ld_index=gwas_index,
            gnomad_ld=gnomad_ld,
            L=max_causal_snps,
            session=session,
            studyId=studyId,
            region=region,
            locusStart=int(locusStart),
            locusEnd=int(locusEnd),
            marginals=marginals,
            study_type=study[0]["studyType"],
            quality_controls=study_locus_row["qualityControls"],
            susie_est_tausq=susie_est_tausq,
            run_carma=run_carma,
            run_sumstat_imputation=run_sumstat_imputation,
            carma_time_limit=carma_time_limit,
            carma_tau=carma_tau,
            imputed_r2_threshold=imputed_r2_threshold,
            ld_score_threshold=ld_score_threshold,
            lead_pval_threshold=lead_pval_threshold,
            purity_mean_r2_threshold=purity_mean_r2_threshold,
            purity_min_r2_threshold=purity_min_r2_threshold,
            cs_lbf_thr=cs_lbf_thr,
            ld_min_r2=ld_min_r2,
            N_total=N_total,
        )

        return out
