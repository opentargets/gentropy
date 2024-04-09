"""Step to run a finemapping using."""

from __future__ import annotations

import time
from typing import Any

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Row, Window
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.datasource.gnomad.ld import GnomADLDMatrix
from gentropy.method.carma import CARMA
from gentropy.method.sumstat_imputation import SummaryStatisticsImputation
from gentropy.method.susie_inf import SUSIE_inf


class SusieFineMapperStep:
    """SuSie finemaping. It has generic methods to run SuSie fine mapping for a study locus.

    This class/step is the temporary solution of the fine-mapping warpper for the development purposes.
    In the future this step will be refactored and moved to the methods module.
    """

    @staticmethod
    def susie_finemapper_one_studylocus_row(
        GWAS: SummaryStatistics,
        session: Session,
        study_locus_row: Row,
        study_index: StudyIndex,
        window: int = 1_000_000,
        L: int = 10,
    ) -> StudyLocus:
        """Susie fine-mapper for StudyLocus row with SummaryStatistics object.

        Args:
            GWAS (SummaryStatistics): GWAS summary statistics
            session (Session): Spark session
            study_locus_row (Row): StudyLocus row
            study_index (StudyIndex): StudyIndex object
            window (int): window size for fine-mapping
            L (int): number of causal variants

        Returns:
            StudyLocus: StudyLocus object with fine-mapped credible sets
        """
        # PLEASE DO NOT REMOVE THIS LINE
        pd.DataFrame.iteritems = pd.DataFrame.items

        chromosome = study_locus_row["chromosome"]
        position = study_locus_row["position"]
        studyId = study_locus_row["studyId"]

        study_index_df = study_index._df
        study_index_df = study_index_df.filter(f.col("studyId") == studyId)
        major_population = study_index_df.select(
            "studyId",
            f.array_max(f.col("ldPopulationStructure"))
            .getItem("ldPopulation")
            .alias("majorPopulation"),
        ).collect()[0]["majorPopulation"]

        region = (
            chromosome
            + ":"
            + str(int(position - window / 2))
            + "-"
            + str(int(position + window / 2))
        )

        gwas_df = (
            GWAS.df.withColumn("z", f.col("beta") / f.col("standardError"))
            .withColumn("chromosome", f.split(f.col("variantId"), "_")[0])
            .withColumn("position", f.split(f.col("variantId"), "_")[1])
            .filter(f.col("studyId") == studyId)
            .filter(f.col("z").isNotNull())
        )

        ld_index = (
            GnomADLDMatrix()
            .get_locus_index(
                study_locus_row=study_locus_row,
                window_size=window,
                major_population=major_population,
            )
            .withColumn(
                "variantId",
                f.concat(
                    f.lit(chromosome),
                    f.lit("_"),
                    f.col("`locus.position`"),
                    f.lit("_"),
                    f.col("alleles").getItem(0),
                    f.lit("_"),
                    f.col("alleles").getItem(1),
                ).cast("string"),
            )
        )

        # Filtering out the variants that are not in the LD matrix, we don't need them
        gwas_index = gwas_df.join(
            ld_index.select("variantId", "alleles", "idx"), on="variantId"
        ).sort("idx")

        gnomad_ld = GnomADLDMatrix.get_numpy_matrix(
            gwas_index, gnomad_ancestry=major_population
        )

        pd_df = gwas_index.toPandas()
        z_to_fm = np.array(pd_df["z"])
        ld_to_fm = gnomad_ld

        susie_output = SUSIE_inf.susie_inf(z=z_to_fm, LD=ld_to_fm, L=L)

        schema = StructType(
            [
                StructField("variantId", StringType(), True),
                StructField("chromosome", StringType(), True),
                StructField("position", IntegerType(), True),
            ]
        )
        pd_df["position"] = pd_df["position"].astype(int)
        variant_index = session.spark.createDataFrame(
            pd_df[
                [
                    "variantId",
                    "chromosome",
                    "position",
                ]
            ],
            schema=schema,
        )

        return SusieFineMapperStep.susie_inf_to_studylocus(
            susie_output=susie_output,
            session=session,
            studyId=studyId,
            region=region,
            variant_index=variant_index,
        )

    @staticmethod
    def susie_inf_to_studylocus(
        susie_output: dict[str, Any],
        session: Session,
        studyId: str,
        region: str,
        variant_index: DataFrame,
        cs_lbf_thr: float = 2,
        sum_pips: float = 0.99,
    ) -> StudyLocus:
        """Convert SuSiE-inf output to StudyLocus DataFrame.

        Args:
            susie_output (dict[str, Any]): SuSiE-inf output dictionary
            session (Session): Spark session
            studyId (str): study ID
            region (str): region
            variant_index (DataFrame): DataFrame with variant information
            cs_lbf_thr (float): credible set logBF threshold, default is 2
            sum_pips (float): the expected sum of posterior probabilities in the locus, default is 0.99 (99% credible set)

        Returns:
            StudyLocus: StudyLocus object with fine-mapped credible sets
        """
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
        cred_sets = None
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
                .sort(f.desc("posteriorProbability"))
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
                        "finemappingMethod": f.lit("SuSiE-inf"),
                    }
                )
                .withColumn(
                    "studyLocusId",
                    StudyLocus.assign_study_locus_id(
                        f.col("studyId"), f.col("variantId")
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
            if cred_sets is None:
                cred_sets = cred_set
            else:
                cred_sets = cred_sets.unionByName(cred_set)
        return StudyLocus(
            _df=cred_sets,
            _schema=StudyLocus.get_schema(),
        )

    @staticmethod
    def susie_finemapper_ss_gathered(
        session: Session,
        study_locus_row: Row,
        study_index: StudyIndex,
        window: int = 1_000_000,
        L: int = 10,
    ) -> StudyLocus:
        """Susie fine-mapper for StudyLocus row with locus annotated summary statistics.

        Args:
            session (Session): Spark session
            study_locus_row (Row): StudyLocus row
            study_index (StudyIndex): StudyIndex object
            window (int): window size for fine-mapping
            L (int): number of causal variants

        Returns:
            StudyLocus: StudyLocus object with fine-mapped credible sets
        """
        # PLEASE DO NOT REMOVE THIS LINE
        pd.DataFrame.iteritems = pd.DataFrame.items

        chromosome = study_locus_row["chromosome"]
        position = study_locus_row["position"]
        studyId = study_locus_row["studyId"]

        study_index_df = study_index._df
        study_index_df = study_index_df.filter(f.col("studyId") == studyId)
        major_population = study_index_df.select(
            "studyId",
            f.array_max(f.col("ldPopulationStructure"))
            .getItem("ldPopulation")
            .alias("majorPopulation"),
        ).collect()[0]["majorPopulation"]

        region = (
            chromosome
            + ":"
            + str(int(position - window / 2))
            + "-"
            + str(int(position + window / 2))
        )

        gwas_df = (
            session.spark.createDataFrame(study_locus_row.locus)
            .withColumn("z", f.col("beta") / f.col("standardError"))
            .withColumn("chromosome", f.split(f.col("variantId"), "_")[0])
            .withColumn("position", f.split(f.col("variantId"), "_")[1])
            .filter(f.col("z").isNotNull())
        )

        ld_index = (
            GnomADLDMatrix()
            .get_locus_index(
                study_locus_row=study_locus_row,
                window_size=window,
                major_population=major_population,
            )
            .withColumn(
                "variantId",
                f.concat(
                    f.lit(chromosome),
                    f.lit("_"),
                    f.col("`locus.position`"),
                    f.lit("_"),
                    f.col("alleles").getItem(0),
                    f.lit("_"),
                    f.col("alleles").getItem(1),
                ).cast("string"),
            )
        )

        # Filtering out the variants that are not in the LD matrix, we don't need them
        gwas_index = gwas_df.join(
            ld_index.select("variantId", "alleles", "idx"), on="variantId"
        ).sort("idx")

        gnomad_ld = GnomADLDMatrix.get_numpy_matrix(
            gwas_index, gnomad_ancestry=major_population
        )

        pd_df = gwas_index.toPandas()
        z_to_fm = np.array(pd_df["z"])
        ld_to_fm = gnomad_ld

        susie_output = SUSIE_inf.susie_inf(z=z_to_fm, LD=ld_to_fm, L=L)

        schema = StructType(
            [
                StructField("variantId", StringType(), True),
                StructField("chromosome", StringType(), True),
                StructField("position", IntegerType(), True),
            ]
        )
        pd_df["position"] = pd_df["position"].astype(int)
        variant_index = session.spark.createDataFrame(
            pd_df[
                [
                    "variantId",
                    "chromosome",
                    "position",
                ]
            ],
            schema=schema,
        )

        return SusieFineMapperStep.susie_inf_to_studylocus(
            susie_output=susie_output,
            session=session,
            studyId=studyId,
            region=region,
            variant_index=variant_index,
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
        susie_est_tausq: bool = False,
        run_carma: bool = False,
        run_sumstat_imputation: bool = False,
        carma_time_limit: int = 600,
        imputed_r2_threshold: float = 0.8,
        ld_score_threshold: float = 4,
        sum_pips: float = 0.99,
    ) -> dict[str, Any]:
        """Susie fine-mapper function that uses LD, z-scores, variant info and other options for Fine-Mapping.

        Args:
            GWAS_df (DataFrame): GWAS DataFrame with mandotary columns: z, variantId
            ld_index (DataFrame): LD index DataFrame
            gnomad_ld (np.ndarray): GnomAD LD matrix
            L (int): number of causal variants
            session (Session): Spark session
            studyId (str): study ID
            region (str): region
            susie_est_tausq (bool): estimate tau squared, default is False
            run_carma (bool): run CARMA, default is False
            run_sumstat_imputation (bool): run summary statistics imputation, default is False
            carma_time_limit (int): CARMA time limit, default is 600 seconds
            imputed_r2_threshold (float): imputed R2 threshold, default is 0.8
            ld_score_threshold (float): LD score threshold ofr imputation, default is 4
            sum_pips (float): the expected sum of posterior probabilities in the locus, default is 0.99 (99% credible set)

        Returns:
            dict[str, Any]: dictionary with study locus, number of GWAS variants, number of LD variants, number of variants after merge, number of outliers, number of imputed variants, number of variants to fine-map
        """
        # PLEASE DO NOT REMOVE THIS LINE
        pd.DataFrame.iteritems = pd.DataFrame.items

        start_time = time.time()
        GWAS_df = GWAS_df.toPandas()
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
                z=z_to_fm, ld=ld_to_fm, sec_threshold=carma_time_limit
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
            z=z_to_fm, LD=ld_to_fm, L=L, est_tausq=susie_est_tausq
        )

        schema = StructType([StructField("variantId", StringType(), True)])
        variant_index = (
            session.spark.createDataFrame(
                GWAS_df[["variantId"]],
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
        )

        end_time = time.time()

        log_df = pd.DataFrame(
            {
                "N_gwas": N_gwas,
                "N_ld": N_ld,
                "N_overlap": N_after_merge,
                "N_outliers": N_outliers,
                "N_imputed": N_imputed,
                "N_final_to_fm": len(ld_to_fm),
                "eleapsed_time": end_time - start_time,
            },
            index=[0],
        )

        return {
            "study_locus": study_locus,
            "log": log_df,
        }

    @staticmethod
    def susie_finemapper_one_studylocus_row_v2_dev(
        GWAS: SummaryStatistics,
        session: Session,
        study_locus_row: Row,
        study_index: StudyIndex,
        window: int = 1_000_000,
        L: int = 10,
        susie_est_tausq: bool = False,
        run_carma: bool = False,
        run_sumstat_imputation: bool = False,
        carma_time_limit: int = 600,
        imputed_r2_threshold: float = 0.8,
        ld_score_threshold: float = 4,
        sum_pips: float = 0.99,
    ) -> dict[str, Any]:
        """Susie fine-mapper function that uses Summary Statstics, chromosome and position as inputs.

        Args:
            GWAS (SummaryStatistics): GWAS summary statistics
            session (Session): Spark session
            study_locus_row (Row): StudyLocus row
            study_index (StudyIndex): StudyIndex object
            window (int): window size for fine-mapping
            L (int): number of causal variants
            susie_est_tausq (bool): estimate tau squared, default is False
            run_carma (bool): run CARMA, default is False
            run_sumstat_imputation (bool): run summary statistics imputation, default is False
            carma_time_limit (int): CARMA time limit, default is 600 seconds
            imputed_r2_threshold (float): imputed R2 threshold, default is 0.8
            ld_score_threshold (float): LD score threshold ofr imputation, default is 4
            sum_pips (float): the expected sum of posterior probabilities in the locus, default is 0.99 (99% credible set)

        Returns:
            dict[str, Any]: dictionary with study locus, number of GWAS variants, number of LD variants, number of variants after merge, number of outliers, number of imputed variants, number of variants to fine-map
        """
        # PLEASE DO NOT REMOVE THIS LINE
        pd.DataFrame.iteritems = pd.DataFrame.items

        chromosome = study_locus_row["chromosome"]
        position = study_locus_row["position"]
        studyId = study_locus_row["studyId"]

        study_index_df = study_index._df
        study_index_df = study_index_df.filter(f.col("studyId") == studyId)
        major_population = study_index_df.select(
            "studyId",
            f.array_max(f.col("ldPopulationStructure"))
            .getItem("ldPopulation")
            .alias("majorPopulation"),
        ).collect()[0]["majorPopulation"]

        region = (
            chromosome
            + ":"
            + str(int(position - window / 2))
            + "-"
            + str(int(position + window / 2))
        )
        gwas_df = (
            GWAS.df.withColumn("z", f.col("beta") / f.col("standardError"))
            .withColumn(
                "chromosome", f.split(f.col("variantId"), "_")[0].cast("string")
            )
            .withColumn("position", f.split(f.col("variantId"), "_")[1].cast("int"))
            .filter(f.col("studyId") == studyId)
            .filter(f.col("z").isNotNull())
            .filter(f.col("chromosome") == chromosome)
            .filter(f.col("position") >= position - window / 2)
            .filter(f.col("position") <= position + window / 2)
        )

        ld_index = (
            GnomADLDMatrix()
            .get_locus_index(
                study_locus_row=study_locus_row,
                window_size=window,
                major_population=major_population,
            )
            .withColumn(
                "variantId",
                f.concat(
                    f.lit(chromosome),
                    f.lit("_"),
                    f.col("`locus.position`"),
                    f.lit("_"),
                    f.col("alleles").getItem(0),
                    f.lit("_"),
                    f.col("alleles").getItem(1),
                ).cast("string"),
            )
        )

        gnomad_ld = GnomADLDMatrix.get_numpy_matrix(
            ld_index, gnomad_ancestry=major_population
        )

        out = SusieFineMapperStep.susie_finemapper_from_prepared_dataframes(
            GWAS_df=gwas_df,
            ld_index=ld_index,
            gnomad_ld=gnomad_ld,
            L=L,
            session=session,
            studyId=studyId,
            region=region,
            susie_est_tausq=susie_est_tausq,
            run_carma=run_carma,
            run_sumstat_imputation=run_sumstat_imputation,
            carma_time_limit=carma_time_limit,
            imputed_r2_threshold=imputed_r2_threshold,
            ld_score_threshold=ld_score_threshold,
            sum_pips=sum_pips,
        )

        return out
