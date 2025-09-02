"""Step to run a finemapping using."""

from __future__ import annotations

import logging
import time
from typing import Any

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
import scipy as sc
from pyspark.sql import DataFrame, Row, Window
from pyspark.sql.functions import desc, row_number
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from gentropy.common.session import Session
from gentropy.common.spark import order_array_of_structs_by_field
from gentropy.common.stats import pvalue_from_neglogpval
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import (
    FinemappingMethod,
    StudyLocus,
    StudyLocusQualityCheck,
)
from gentropy.method.carma import CARMA
from gentropy.method.ld import LDAnnotator
from gentropy.method.ld_matrix_interface import LDMatrixInterface
from gentropy.method.sumstat_imputation import SummaryStatisticsImputation
from gentropy.method.susie_inf import SUSIE_inf


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
        sum_pips: float = 0.99,
        susie_est_tausq: bool = False,
        run_carma: bool = False,
        run_sumstat_imputation: bool = False,
        carma_time_limit: int = 600,
        carma_tau: float = 0.15,
        imputed_r2_threshold: float = 0.9,
        ld_score_threshold: float = 5,
        ld_min_r2: float = 0.8,
        ignore_qc: bool = False,
    ) -> None:
        """Run fine-mapping on a studyLocusId from a collected studyLocus table.

        Method require a `study_locus_manifest_path` file that will contain ["study_locus_input", "study_locus_output", "log_file"]. `log_file`
        is optional parameter to the manifest. In case it does not exist, the logs from the finemapper are saved under the same directory
        as the `study_locus_output` with `.log` suffix.
        Each execution of the method will only evaluate a single row from the `study_locus_manifest` that is inferred from the `study_locus_index`
        variable.

        Args:
            session (Session): Spark session
            study_index_path (str): path to the study index
            study_locus_manifest_path (str): Path to the CSV manifest containing all study locus input and output locations. Should contain two columns: study_locus_input and study_locus_output
            study_locus_index (int): Index (0-based) of the locus in the manifest to process in this call
            ld_matrix_paths (dict[str, str]): Dictionary with paths to LD matrices
            max_causal_snps (int): Maximum number of causal variants in locus, default is 10
            lead_pval_threshold (float): p-value threshold for the lead variant from CS, default is 1e-5
            purity_mean_r2_threshold (float): threshold for purity mean r2 qc metrics for filtering credible sets, default is 0
            purity_min_r2_threshold (float): threshold for purity min r2 qc metrics for filtering credible sets, default is 0.25
            cs_lbf_thr (float): credible set logBF threshold for filtering credible sets, default is 2
            sum_pips (float): the expected sum of posterior probabilities in the locus, default is 0.99 (99% credible set)
            susie_est_tausq (bool): estimate tau squared, default is False
            run_carma (bool): run CARMA, default is False
            run_sumstat_imputation (bool): run summary statistics imputation, default is False
            carma_time_limit (int): CARMA time limit, default is 600 seconds
            carma_tau (float): CARMA tau, shrinkage parameter
            imputed_r2_threshold (float): imputed R2 threshold, default is 0.9
            ld_score_threshold (float): LD score threshold ofr imputation, default is 5
            ld_min_r2 (float): Threshold to filter CS by leads in high LD, default is 0.8
            ignore_qc (bool): run SuSiE regardless of QC flags, default is False
        """
        # Read locus manifest.
        study_locus_manifest = pd.read_csv(study_locus_manifest_path)
        row = study_locus_manifest.loc[study_locus_index]
        study_locus_input = row["study_locus_input"]
        study_locus_output = row["study_locus_output"]
        log_output = study_locus_output + ".log"
        if "log_output" in study_locus_manifest.columns:
            log_output = row["log_output"] + ".log"

        # Read studyLocus
        study_locus = (
            StudyLocus.from_parquet(session, study_locus_input)
            .df.withColumn(
                "studyLocusId",
                StudyLocus.assign_study_locus_id(
                    ["studyId", "variantId", "finemappingMethod"]
                ),
            )
            .collect()[0]
        )
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
            sum_pips=sum_pips,
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
            ignore_qc=ignore_qc,
        )

        if result_logging is not None:
            if result_logging["study_locus"] is not None:
                # Write result
                df: DataFrame = result_logging["study_locus"].df
                df = df.withColumn("qualityControls", f.lit(None))
                df = df.withColumn(
                    "qualityControls",
                    StudyLocus.update_quality_flag(
                        f.col("qualityControls"),
                        f.lit(True),
                        StudyLocusQualityCheck.OUT_OF_SAMPLE_LD,
                    ),
                )
                df.coalesce(session.output_partitions).write.mode(
                    session.write_mode
                ).parquet(study_locus_output)
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
    def susie_inf_to_studylocus(  # noqa: C901
        susie_output: dict[str, Any],
        session: Session,
        studyId: str,
        region: str,
        variant_index: DataFrame,
        ld_matrix: np.ndarray,
        locusStart: int,
        locusEnd: int,
        cs_lbf_thr: float = 2,
        sum_pips: float = 0.99,
        lead_pval_threshold: float = 1,
        purity_mean_r2_threshold: float = 0,
        purity_min_r2_threshold: float = 0,
        ld_min_r2: float = 0.9,
    ) -> StudyLocus | None:
        """Convert SuSiE-inf output to StudyLocus DataFrame.

        Args:
            susie_output (dict[str, Any]): SuSiE-inf output dictionary
            session (Session): Spark session
            studyId (str): study ID
            region (str): region
            variant_index (DataFrame): DataFrame with variant information
            ld_matrix (np.ndarray): LD matrix used for fine-mapping
            locusStart (int): locus start
            locusEnd (int): locus end
            cs_lbf_thr (float): credible set logBF threshold for filtering credible sets, default is 2
            sum_pips (float): the expected sum of posterior probabilities in the locus, default is 0.99 (99% credible set)
            lead_pval_threshold (float): p-value threshold for the lead variant from CS
            purity_mean_r2_threshold (float): thrshold for purity mean r2 qc metrics for filtering credible sets
            purity_min_r2_threshold (float): thrshold for purity min r2 qc metrics for filtering credible sets
            ld_min_r2 (float): Threshold to fillter CS by leads in high LD, default is 0.9

        Returns:
            StudyLocus | None: StudyLocus object with fine-mapped credible sets
        """
        # PLEASE DO NOT REMOVE THIS LINE
        pd.DataFrame.iteritems = pd.DataFrame.items

        variants = np.array(
            [row["variantId"] for row in variant_index.select("variantId").collect()]
        ).reshape(-1, 1)

        PIPs = susie_output["PIP"]
        lbfs = susie_output["lbf_variable"]
        mu = susie_output["mu"]
        susie_result = np.hstack((variants, PIPs, lbfs, mu))

        L_snps = PIPs.shape[1]

        # Extracting credible sets
        order_creds = list(enumerate(susie_output["lbf"]))
        order_creds.sort(key=lambda x: x[1], reverse=True)

        counter = 0
        for i, cs_lbf_value in order_creds:
            if counter > 0 and cs_lbf_value < cs_lbf_thr:
                counter += 1
                continue
            counter += 1
            sorted_arr = susie_result[
                susie_result[:, i + 1].astype(float).argsort()[::-1]
            ]
            cumsum_arr = np.cumsum(sorted_arr[:, i + 1].astype(float))
            filter_row = np.argmax(cumsum_arr >= sum_pips)
            if filter_row == 0 and cumsum_arr[0] < sum_pips:
                filter_row = len(cumsum_arr)
            filter_row += 1
            filtered_arr = sorted_arr[:filter_row]
            cred_set = filtered_arr[:, [0, i + 1, i + L_snps + 1, i + 2 * L_snps + 1]]
            win = Window.rowsBetween(
                Window.unboundedPreceding, Window.unboundedFollowing
            )

            cred_set = (
                session.spark.createDataFrame(
                    cred_set.tolist(),
                    ["variantId", "posteriorProbability", "logBF", "beta"],
                )
                .join(
                    variant_index.select(
                        "variantId",
                        "chromosome",
                        "position",
                    ),
                    "variantId",
                )
                .sort(f.desc(f.col("posteriorProbability").cast("double")))
                .withColumn(
                    "locus",
                    f.collect_list(
                        f.struct(
                            f.col("variantId").cast("string").alias("variantId"),
                            f.col("posteriorProbability")
                            .cast("double")
                            .alias("posteriorProbability"),
                            f.col("logBF").cast("double").alias("logBF"),
                            f.col("beta").cast("double").alias("beta"),
                        )
                    ).over(win),
                )
                .limit(1)
                .withColumns(
                    {
                        "studyId": f.lit(studyId),
                        "region": f.lit(region),
                        "credibleSetIndex": f.lit(counter),
                        "credibleSetlog10BF": f.lit(cs_lbf_value * 0.4342944819),
                        "finemappingMethod": f.lit(FinemappingMethod.SUSIE_INF.value),
                    }
                )
                .withColumn(
                    "studyLocusId",
                    StudyLocus.assign_study_locus_id(
                        ["studyId", "variantId", "finemappingMethod"]
                    ),
                )
                .select(
                    "studyLocusId",
                    "studyId",
                    "region",
                    "credibleSetIndex",
                    "locus",
                    "variantId",
                    "chromosome",
                    "position",
                    "finemappingMethod",
                    "credibleSetlog10BF",
                )
            )
            if counter == 1:
                cred_sets = cred_set
            else:
                cred_sets = cred_sets.unionByName(cred_set)

        # Calulating purity
        variant_index_df = variant_index.toPandas()
        cred_sets_variantId = cred_sets.select("locus.variantId").toPandas()

        lead_variantId_list = (
            cred_sets.select("variantId").toPandas()["variantId"].tolist()
        )
        cred_set_index = (
            cred_sets.select("credibleSetIndex").toPandas()["credibleSetIndex"].tolist()
        )
        vlist_series = pd.Series(lead_variantId_list)
        ind = vlist_series.map(variant_index_df.set_index("variantId").index.get_loc)
        z_values = variant_index_df.iloc[ind]["z"].tolist()
        z_values_array = np.array(z_values)
        pval = sc.stats.chi2.sf((z_values_array**2), 1)

        # sometimes pval is 0, we need to avoid it
        pval[pval < 1e-322] = 1e-322

        neglogpval = -np.log10(pval)
        neglogpval = neglogpval.tolist()

        list_purity_mean_r2 = []
        list_purity_min_r2 = []
        for _, row in cred_sets_variantId.iterrows():
            row = row.iloc[0]
            vlist_series = pd.Series(row)
            ind = vlist_series.map(
                variant_index_df.set_index("variantId").index.get_loc
            )
            # print(variant_index_df.iloc[ind,0]==vlist)
            squared_matrix = ld_matrix[ind, :][:, ind] ** 2
            purity_mean_r2 = np.mean(squared_matrix)
            purity_min_r2 = np.min(squared_matrix)
            list_purity_mean_r2.append(purity_mean_r2)
            list_purity_min_r2.append(purity_min_r2)

        cred_sets = cred_sets.drop("pValueMantissa", "pValueExponent")

        df = pd.DataFrame(
            {
                "credibleSetIndex": cred_set_index,
                "purityMeanR2": list_purity_mean_r2,
                "purityMinR2": list_purity_min_r2,
                "zScore": z_values,
                "neglogpval": neglogpval,
            }
        )
        schema = StructType(
            [
                StructField("credibleSetIndex", IntegerType(), True),
                StructField("purityMeanR2", DoubleType(), True),
                StructField("purityMinR2", DoubleType(), True),
                StructField("zScore", DoubleType(), True),
                StructField("neglogpval", DoubleType(), True),
            ]
        )

        df_spark = session.spark.createDataFrame(df, schema=schema)

        cred_sets = cred_sets.join(df_spark, on="credibleSetIndex")

        mantissa, exponent = pvalue_from_neglogpval(cred_sets.neglogpval)
        cred_sets = cred_sets.withColumn("pValueMantissa", mantissa)
        cred_sets = cred_sets.withColumn("pValueExponent", exponent)
        cred_sets = cred_sets.withColumn(
            "pValueMantissa", f.col("pValueMantissa").cast("float")
        )

        # Filter by lead p-value, credible set logBF, purity mean r2 and purity min r2
        cred_sets = cred_sets.filter(
            (f.col("neglogpval") >= -np.log10(lead_pval_threshold))
            & (f.col("credibleSetlog10BF") >= cs_lbf_thr * 0.4342944819)
            & (~f.isnan(f.col("credibleSetlog10BF")))
            & (f.col("purityMinR2") >= purity_min_r2_threshold)
            & (f.col("purityMeanR2") >= purity_mean_r2_threshold)
        )

        if cred_sets.count() == 0:
            return None

        # Remove duplicated by lead variant
        if cred_sets.count() > 1:
            window = Window.partitionBy("variantId").orderBy("credibleSetIndex")
            cred_sets = cred_sets.withColumn("rank", row_number().over(window))
            cred_sets = cred_sets.filter(cred_sets["rank"] == 1).drop("rank")
            cred_sets = cred_sets.orderBy("credibleSetIndex")

        # Remove CSs with high LD between leads
        if cred_sets.count() > 1:
            cred_sets = cred_sets.orderBy(desc("neglogpval"))
            lead_variantId_list = (
                cred_sets.select("variantId").toPandas()["variantId"].tolist()
            )
            vlist_series = pd.Series(lead_variantId_list)
            ind = vlist_series.map(
                variant_index_df.set_index("variantId").index.get_loc
            )
            ld_leads = ld_matrix[ind, :][:, ind]
            ld_leads = ld_leads**2
            ld_leads = ld_leads - np.tril(ld_leads)
            np.fill_diagonal(ld_leads, -1)

            lead_variantId_list_to_delete: list[str] = []
            for idx in range(len(lead_variantId_list)):
                vId = lead_variantId_list[idx]
                if vId in lead_variantId_list_to_delete:
                    continue
                high_ld_indices = np.where(ld_leads[idx, :] >= ld_min_r2)[0]
                if len(high_ld_indices) > 0:
                    lead_variantId_list_to_delete = (
                        lead_variantId_list_to_delete
                        + list(np.array(lead_variantId_list)[high_ld_indices])
                    )
            if len(lead_variantId_list_to_delete) > 0:
                for vId in lead_variantId_list_to_delete:
                    cred_sets = cred_sets.filter(f.col("variantId") != vId)

        cred_sets = cred_sets.drop("neglogpval")
        cred_sets = cred_sets.withColumn("locusStart", f.lit(locusStart))
        cred_sets = cred_sets.withColumn("locusEnd", f.lit(locusEnd))

        cred_sets = cred_sets.drop("beta").withColumn(
            "beta",
            f.expr("""
                filter(locus, x -> x.variantId = variantId)[0].beta
            """),
        )

        return StudyLocus(
            _df=cred_sets,
            _schema=StudyLocus.get_schema(),
        )

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
        susie_est_tausq: bool = False,
        run_carma: bool = False,
        run_sumstat_imputation: bool = False,
        carma_time_limit: int = 600,
        carma_tau: float = 0.04,
        imputed_r2_threshold: float = 0.8,
        ld_score_threshold: float = 4,
        sum_pips: float = 0.99,
        lead_pval_threshold: float = 1e-5,
        purity_mean_r2_threshold: float = 0,
        purity_min_r2_threshold: float = 0.25,
        cs_lbf_thr: float = 2,
        ld_min_r2: float = 0.9,
        N_total: int = 100_000,
    ) -> dict[str, Any] | None:
        """Susie fine-mapper function that uses LD, z-scores, variant info and other options for Fine-Mapping.

        Args:
            GWAS_df (DataFrame): GWAS DataFrame with mandotary columns: z, variantId
            ld_index (DataFrame): LD index DataFrame
            gnomad_ld (np.ndarray): GnomAD LD matrix
            L (int): number of causal variants
            session (Session): Spark session
            studyId (str): study ID
            region (str): region
            locusStart (int): locus start
            locusEnd (int): locus end
            susie_est_tausq (bool): estimate tau squared, default is False
            run_carma (bool): run CARMA, default is False
            run_sumstat_imputation (bool): run summary statistics imputation, default is False
            carma_time_limit (int): CARMA time limit, default is 600 seconds
            carma_tau (float): CARMA tau, shrinkage parameter
            imputed_r2_threshold (float): imputed R2 threshold, default is 0.8
            ld_score_threshold (float): LD score threshold ofr imputation, default is 4
            sum_pips (float): the expected sum of posterior probabilities in the locus, default is 0.99 (99% credible set)
            lead_pval_threshold (float): p-value threshold for the lead variant from CS, default is 1e-5
            purity_mean_r2_threshold (float): thrshold for purity mean r2 qc metrics for filtering credible sets
            purity_min_r2_threshold (float): thrshold for purity min r2 qc metrics for filtering credible sets
            cs_lbf_thr (float): credible set logBF threshold for filtering credible sets, default is 2
            ld_min_r2 (float): Threshold to fillter CS by leads in high LD, default is 0.9
            N_total (int): total number of samples, default is 100_000

        Returns:
            dict[str, Any] | None: dictionary with study locus, number of GWAS variants, number of LD variants, number of variants after merge, number of outliers, number of imputed variants, number of variants to fine-map
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

        schema = StructType(
            [
                StructField("variantId", StringType(), True),
                StructField("z", DoubleType(), True),
            ]
        )
        variant_index = (
            session.spark.createDataFrame(
                GWAS_df[["variantId", "z"]],
                schema=schema,
            )
            .withColumn(
                "chromosome", f.split(f.col("variantId"), "_")[0].cast("string")
            )
            .withColumn("position", f.split(f.col("variantId"), "_")[1].cast("int"))
        )

        study_locus = SusieFineMapperStep.susie_inf_to_studylocus(
            susie_output=susie_output,
            session=session,
            studyId=studyId,
            region=region,
            variant_index=variant_index,
            sum_pips=sum_pips,
            ld_matrix=ld_to_fm,
            lead_pval_threshold=lead_pval_threshold,
            purity_mean_r2_threshold=purity_mean_r2_threshold,
            purity_min_r2_threshold=purity_min_r2_threshold,
            cs_lbf_thr=cs_lbf_thr,
            ld_min_r2=ld_min_r2,
            locusStart=locusStart,
            locusEnd=locusEnd,
        )

        end_time = time.time()

        if study_locus is not None:
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
                    "number_of_CS": study_locus.df.count(),
                    "error": "",
                },
                index=[0],
            )
        else:
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
                    "number_of_CS": 0,
                    "error": "",
                },
                index=[0],
            )

        return {
            "study_locus": study_locus,
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
        sum_pips: float = 0.99,
        lead_pval_threshold: float = 1e-5,
        purity_mean_r2_threshold: float = 0,
        purity_min_r2_threshold: float = 0.25,
        cs_lbf_thr: float = 2,
        ld_min_r2: float = 0.9,
        log_output: str = "",
        ignore_qc: bool = False,
    ) -> dict[str, Any] | None:
        """Susie fine-mapper function that uses study-locus row with collected locus, chromosome and position as inputs.

        Args:
            session (Session): Spark session
            study_locus_row (Row): StudyLocus row with collected locus
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
            sum_pips (float): the expected sum of posterior probabilities in the locus, default is 0.99 (99% credible set)
            lead_pval_threshold (float): p-value threshold for the lead variant from CS, default is 1e-5
            purity_mean_r2_threshold (float): thrshold for purity mean r2 qc metrics for filtering credible sets
            purity_min_r2_threshold (float): thrshold for purity min r2 qc metrics for filtering credible sets
            cs_lbf_thr (float): credible set logBF threshold for filtering credible sets, default is 2
            ld_min_r2 (float): Threshold to fillter CS by leads in high LD, default is 0.9
            log_output (str): path to the log output
            ignore_qc (bool): run SuSiE regardless of QC flags, default is False

        Returns:
            dict[str, Any] | None: dictionary with study locus, number of GWAS variants, number of LD variants, number of variants after merge, number of outliers, number of imputed variants, number of variants to fine-map, or None
        """
        # PLEASE DO NOT REMOVE THIS LINE
        pd.DataFrame.iteritems = pd.DataFrame.items

        chromosome = study_locus_row["chromosome"]
        studyId = study_locus_row["studyId"]
        locusStart = study_locus_row["locusStart"]
        locusEnd = study_locus_row["locusEnd"]

        study_index_df = study_index._df
        study_index_df = study_index_df.filter(f.col("studyId") == studyId)

        # Desision tree - study index
        if study_index_df.count() == 0:
            if log_output != "":
                SusieFineMapperStep._empty_log_mg(
                    studyId=studyId,
                    region="",
                    error_mg="No study index found for the studyId",
                    path_out=log_output,
                )
            logging.warning("No study index found for the studyId")
            return None

        major_population = (
            study_index_df.select(
                "studyId",
                order_array_of_structs_by_field(
                    "ldPopulationStructure", "relativeSampleSize"
                ).alias("ldPopulationStructure"),
            )
            .withColumn(
                "majorPopulation",
                f.when(
                    f.col("ldPopulationStructure").isNotNull(),
                    LDAnnotator._get_major_population(f.col("ldPopulationStructure")),
                ),
            )
            .collect()[0]["majorPopulation"]
        )

        # This is a temporary solution
        if major_population == "eas":
            major_population = "csa"

        N_total = int(study_index_df.select("nSamples").collect()[0]["nSamples"])
        if N_total is None:
            N_total = 100_000

        locusStart = max(locusStart, 0)

        region = chromosome + ":" + str(int(locusStart)) + "-" + str(int(locusEnd))

        # Desision tree - studyType
        if study_index_df.select("studyType").collect()[0]["studyType"] not in [
            "gwas",
            "pqtl",
        ]:
            if log_output != "":
                SusieFineMapperStep._empty_log_mg(
                    studyId=studyId,
                    region=region,
                    error_mg="Study type is not GWAS or non gwas catalog pqtl",
                    path_out=log_output,
                )
            logging.warning("Study type is not GWAS or non gwas catalog pqtl")
            if not ignore_qc:
                return None

        # Desision tree - ancestry
        if major_population not in ["nfe", "csa", "afr"]:
            if log_output != "":
                SusieFineMapperStep._empty_log_mg(
                    studyId=studyId,
                    region=region,
                    error_mg="Major ancestry is not nfe, csa or afr",
                    path_out=log_output,
                )
            logging.warning("Major ancestry is not nfe, csa or afr")
            if not ignore_qc:
                return None

        # Desision tree - hasSumstats
        if not study_index_df.select("hasSumstats").collect()[0]["hasSumstats"]:
            if log_output != "":
                SusieFineMapperStep._empty_log_mg(
                    studyId=studyId,
                    region=region,
                    error_mg="No sumstats found for the studyId",
                    path_out=log_output,
                )
            logging.warning("No sumstats found for the studyId")
            if not ignore_qc:
                return None

        # Desision tree - qulityControls
        keys_reasons = [
            "SMALL_NUMBER_OF_SNPS",
            "FAILED_GC_LAMBDA_CHECK",
            "FAILED_PZ_CHECK",
            "FAILED_MEAN_BETA_CHECK",
            "NO_OT_CURATION",
            "SUMSTATS_NOT_AVAILABLE",
        ]

        qc_mappings_dict = StudyIndex.get_QC_mappings()
        invalid_reasons = [
            qc_mappings_dict[key] for key in keys_reasons if key in qc_mappings_dict
        ]

        x_boolean = (
            study_index_df.withColumn(
                "FailedQC",
                f.arrays_overlap(
                    f.col("qualityControls"),
                    f.array([f.lit(reason) for reason in invalid_reasons]),
                ),
            )
            .select("FailedQC")
            .collect()[0]["FailedQC"]
        )
        if x_boolean:
            if log_output != "":
                SusieFineMapperStep._empty_log_mg(
                    studyId=studyId,
                    region=region,
                    error_mg="Quality control check failed for this study",
                    path_out=log_output,
                )
            logging.warning("Quality control check failed for this study")
            if not ignore_qc:
                return None

        # Desision tree - analysisFlags
        study_index_df = study_index_df.drop("FailedQC")
        invalid_reasons = [
            "Multivariate analysis",
            "ExWAS",
            "Non-additive model",
            "GxG",
            "GxE",
            "Case-case study",
        ]
        x_boolean = (
            study_index_df.withColumn(
                "FailedQC",
                f.arrays_overlap(
                    f.col("analysisFlags"),
                    f.array([f.lit(reason) for reason in invalid_reasons]),
                ),
            )
            .select("FailedQC")
            .collect()[0]["FailedQC"]
        )
        if x_boolean:
            if log_output != "":
                SusieFineMapperStep._empty_log_mg(
                    studyId=studyId,
                    region=region,
                    error_mg="Analysis Flags check failed for this study",
                    path_out=log_output,
                )
            logging.warning("Analysis Flags check failed for this study")
            if not ignore_qc:
                return None

        gwas_df = session.spark.createDataFrame(
            [study_locus_row], StudyLocus.get_schema()
        )
        exploded_df = gwas_df.select(f.explode("locus").alias("locus"))

        result_df = exploded_df.select(
            "locus.variantId", "locus.beta", "locus.standardError"
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

        ld_index = LDMatrixInterface.get_locus_index_boundaries(
            ld_matrix_paths=ld_matrix_paths,
            study_locus_row=study_locus_row,
            ancestry=major_population,
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
                ancestry=major_population
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
                ancestry=major_population
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
            if not ignore_qc:
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
            susie_est_tausq=susie_est_tausq,
            run_carma=run_carma,
            run_sumstat_imputation=run_sumstat_imputation,
            carma_time_limit=carma_time_limit,
            carma_tau=carma_tau,
            imputed_r2_threshold=imputed_r2_threshold,
            ld_score_threshold=ld_score_threshold,
            sum_pips=sum_pips,
            lead_pval_threshold=lead_pval_threshold,
            purity_mean_r2_threshold=purity_mean_r2_threshold,
            purity_min_r2_threshold=purity_min_r2_threshold,
            cs_lbf_thr=cs_lbf_thr,
            ld_min_r2=ld_min_r2,
            N_total=N_total,
        )

        return out
