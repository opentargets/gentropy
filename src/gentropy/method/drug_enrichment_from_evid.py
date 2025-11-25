"""Class to run chembl drug enrichemnt using any evidence as input."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from scipy.stats import chi2, fisher_exact

from gentropy.common.spark import calculate_harmonic_sum
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus


class chemblDrugEnrichment:
    """Chembl drug target enrichment.

    Note: uses the logic from Nealson's paper (doi.org/10.1038/s41586-024-07316-0).
    """

    @staticmethod
    def to_disease_target_evidence(
        table_with_score: DataFrame,
        score_column: str,
        datasource_id: str,
        study_locus: StudyLocus,
        study_index: StudyIndex,
        min_score: float = 0.0,
        datatype_id: str = "GWAS",
    ) -> DataFrame:
        """Convert score from table to disease target evidence.

        The table have to cosist a studyLocusId column.

        Args:
            table_with_score (DataFrame): Table with score
            score_column (str): Column name with score
            datasource_id (str): Data source ID
            study_locus (StudyLocus): Study locus dataset
            study_index (StudyIndex): Study index dataset
            min_score (float): Minimum score to keep
            datatype_id (str): Data type ID

        Returns:
            DataFrame: Disease target evidence
        """
        return (
            table_with_score.filter(f.col(score_column) >= min_score)
            .join(
                study_locus.df.select("studyLocusId", "studyId"),
                on="studyLocusId",
                how="inner",
            )
            .join(
                study_index.df.select("studyId", "diseaseIds"),
                on="studyId",
                how="inner",
            )
            .select(
                f.lit(datatype_id).alias("datatypeId"),
                f.lit(datasource_id).alias("datasourceId"),
                f.col("geneId").alias("targetId"),
                f.explode(f.col("diseaseIds")).alias("diseaseId"),
                f.col(score_column).alias("resourceScore"),
                "studyLocusId",
            )
        )

    @staticmethod
    def selecting_all_decendands_based_on_efo_list(
        disease_index_orig: DataFrame, efo_ids: list[str]
    ) -> list[str]:
        """The function will select all decendands based on efo list.

        Args:
            disease_index_orig (DataFrame): The original disease index (not epxloded)
            efo_ids (list[str]): List of EFO IDs to select decendands for
        Returns:
            list[str]: List of disease IDs
        """
        disease_index = disease_index_orig.select(
            f.col("id").alias("diseaseId"),
            f.explode("ancestors").alias("ancestorDiseaseId"),
        )

        disease_index = disease_index.union(
            disease_index_orig.select(
                f.col("id").alias("diseaseId"),
                f.col("id").alias("ancestorDiseaseId"),
            )
        )

        disease_index_parquet = (
            disease_index.filter(f.col("ancestorDiseaseId").isin(efo_ids))
            .select("diseaseId")
            .distinct()
        )

        disease_index_list = [
            row["diseaseId"] for row in disease_index_parquet.collect()
        ]

        return disease_index_list

    @staticmethod
    def evidence_to_direct_assosiations(
        disease_target_evidence: DataFrame,
        use_max: bool = False,
        efo_to_remove: list[str] | None = None,
    ) -> DataFrame:
        """Convert evidence to direct associations.

        Args:
            disease_target_evidence (DataFrame): Disease target evidence
            use_max (bool): Use max score or harmonic sum (harmonic sum is the default)
            efo_to_remove (list[str] | None): List of EFO IDs to remove

        Returns:
            DataFrame: Direct associations
        """
        if efo_to_remove is not None:
            disease_target_evidence = disease_target_evidence.filter(
                ~f.col("diseaseId").isin(efo_to_remove)
            )

        if use_max:
            return (
                disease_target_evidence.groupBy("targetId", "diseaseId")
                .agg(f.max("resourceScore").alias("direct_assoc_score"))
                .select("targetId", "diseaseId", "direct_assoc_score")
            )
        else:
            return (
                disease_target_evidence.groupBy("targetId", "diseaseId")
                .agg(f.collect_set("resourceScore").alias("scores"))
                .select(
                    "targetId",
                    "diseaseId",
                    calculate_harmonic_sum(f.col("scores")).alias("direct_assoc_score"),
                )
            )

    @staticmethod
    def evidence_to_indirect_assosiations(
        disease_target_evidence: DataFrame,
        disease_index_orig: DataFrame,
        use_max: bool = False,
        efo_to_remove: list[str] | None = None,
    ) -> DataFrame:
        """Convert evidence to indirect associations.

        Args:
            disease_target_evidence (DataFrame): Disease target evidence
            disease_index_orig (DataFrame): The original disease index (not epxloded)
            use_max (bool): Use max score or harmonic sum (harmonic sum is the default)
            efo_to_remove (list[str] | None): List of EFO IDs to remove
        Returns:
            DataFrame: Direct associations
        """
        if efo_to_remove is not None:
            disease_target_evidence = disease_target_evidence.filter(
                ~f.col("diseaseId").isin(efo_to_remove)
            )

        disease_index = disease_index_orig.select(
            f.col("id").alias("diseaseId"),
            f.explode("ancestors").alias("ancestorDiseaseId"),
        )

        disease_index = disease_index.union(
            disease_index_orig.select(
                f.col("id").alias("diseaseId"),
                f.col("id").alias("ancestorDiseaseId"),
            )
        )

        if use_max:
            return (
                disease_target_evidence.join(disease_index, on="diseaseId", how="inner")
                .groupBy("targetId", "ancestorDiseaseId")
                .agg(f.max("resourceScore").alias("indirect_assoc_score"))
                .select("targetId", "ancestorDiseaseId", "indirect_assoc_score")
                .withColumnRenamed("ancestorDiseaseId", "diseaseId")
            )
        else:
            return (
                disease_target_evidence.join(disease_index, on="diseaseId", how="inner")
                .groupBy("targetId", "ancestorDiseaseId")
                .agg(f.collect_set("resourceScore").alias("scores"))
                .select(
                    "targetId",
                    "ancestorDiseaseId",
                    calculate_harmonic_sum(f.col("scores")).alias(
                        "indirect_assoc_score"
                    ),
                )
                .withColumnRenamed("ancestorDiseaseId", "diseaseId")
            )

    @staticmethod
    def process_chembl_evidence(
        chembl_orig: DataFrame, efo_to_remove: list[str] | None = None
    ) -> DataFrame:
        """Process chembl evidence.

        Removes EFO from the list, usualy oncolgy.

        Args:
            chembl_orig (DataFrame): Chembl evidence
            efo_to_remove (list[str] | None): List of EFO IDs to remove
        Returns:
            DataFrame: Processed chembl evidence
        """
        if efo_to_remove is not None:
            chembl_orig = chembl_orig.filter(~f.col("diseaseId").isin(efo_to_remove))

        chembl_evidence_max = (
            chembl_orig.groupBy("targetId", "diseaseId")
            .agg(f.max("clinicalPhase").alias("maxClinicalPhase"))
            .filter(f.col("maxClinicalPhase") > 0.5)
        )

        return chembl_evidence_max

    @staticmethod
    def drug_enrichemnt_from_evidence(
        evid: DataFrame,
        disease_index_orig: DataFrame,
        chembl_orig: DataFrame,
        indirect_assoc_score_thr: float = 0.5,
        efo_ancestors_to_remove: list[str] | None = None,
    ) -> pd.DataFrame:
        """Run chembl drug enrichment from scores.

        Args:
            evid (DataFrame): Evidence table
            disease_index_orig (DataFrame): The original disease index (not epxloded)
            chembl_orig (DataFrame): Chembl evidence
            indirect_assoc_score_thr (float): Minimum score to keep in indirect associations
            efo_ancestors_to_remove (list[str] | None): List of EFO IDs to remove
        Returns:
            pd.DataFrame: Drug enrichment table.
        """
        if efo_ancestors_to_remove is not None:
            efo_to_remove = (
                chemblDrugEnrichment.selecting_all_decendands_based_on_efo_list(
                    disease_index_orig=disease_index_orig,
                    efo_ids=efo_ancestors_to_remove,
                )
            )
        else:
            efo_to_remove = None

        chembl = chemblDrugEnrichment.process_chembl_evidence(
            chembl_orig, efo_to_remove
        )

        evid_indirect = chemblDrugEnrichment.evidence_to_indirect_assosiations(
            evid,
            disease_index_orig,
            use_max=True,
            efo_to_remove=efo_to_remove,
        ).cache()

        evid_indirect_count = evid_indirect.filter(
            f.col("indirect_assoc_score") >= indirect_assoc_score_thr
        ).count()

        joined_data = evid_indirect.join(chembl, ["targetId", "diseaseId"], "right")

        df = joined_data.withColumn(
            "geneticSupport",
            f.when(
                f.col("indirect_assoc_score") >= indirect_assoc_score_thr, True
            ).otherwise(False),
        ).cache()

        phases = [2, 3, 4]
        results = []
        z = 1.96  # 95% confidence interval

        for phase in phases:
            # Calculate N_G and N_negG
            N_G = df.filter(f.col("geneticSupport")).count()
            N_negG = df.filter(~f.col("geneticSupport")).count()

            # Calculate X_G and X_negG
            X_G = df.filter(
                (f.col("geneticSupport")) & (f.col("maxClinicalPhase") >= phase)
            ).count()
            X_negG = df.filter(
                ~(f.col("geneticSupport")) & (f.col("maxClinicalPhase") >= phase)
            ).count()

            # Create the contingency table
            contingency_table = [
                [N_negG - X_negG, X_negG],
                [N_G - X_G, X_G],
            ]

            # Perform Fisher's Exact Test
            odds_ratio, p_value = fisher_exact(contingency_table)

            # Calculate confidence interval for odds ratio

            if not np.any(np.array(contingency_table) == 0):
                ln_or = np.log(odds_ratio)
                se_ln_or = np.sqrt(
                    1 / contingency_table[0][0]
                    + 1 / contingency_table[0][1]
                    + 1 / contingency_table[1][0]
                    + 1 / contingency_table[1][1]
                )

                relative_success = (X_G / N_G) / (X_negG / N_negG)
                ln_rs = np.log(relative_success)
                se_ln_rs = np.sqrt((1 / X_negG) - (1 / N_negG) + (1 / X_G) - (1 / N_G))
                p_value_rs = chi2.sf((ln_rs / se_ln_rs) ** 2, df=1)
            else:
                odds_ratio = 1
                ln_or = 0
                se_ln_or = 0
                relative_success = 1
                ln_rs = 0
                se_ln_rs = 0
                p_value_rs = 1

            ci_ln_low = ln_or - z * se_ln_or
            ci_ln_high = ln_or + z * se_ln_or
            ci_low = np.exp(ci_ln_low)
            ci_high = np.exp(ci_ln_high)

            ci_ln_rs_low = ln_rs - z * se_ln_rs
            ci_ln_rs_high = ln_rs + z * se_ln_rs
            ci_rs_low = np.exp(ci_ln_rs_low)
            ci_rs_high = np.exp(ci_ln_rs_high)
            # Store results
            results.append(
                {
                    "clinicalPhase": str(phase) + "+",
                    "odds_ratio": odds_ratio,
                    "p_value": p_value,
                    "ci_low": ci_low,
                    "ci_high": ci_high,
                    "Relative success": relative_success,
                    "ci_rs_low": ci_rs_low,
                    "ci_rs_high": ci_rs_high,
                    "rs_p_value": p_value_rs,
                    "no_evid-low_clinphase": N_negG - X_negG,
                    "no_evid-high_clinphase": X_negG,
                    "yes_evid-low_clinphase": N_G - X_G,
                    "yes_evid-high_clinphase": X_G,
                    "total_indirect_assoc": evid_indirect_count,
                }
            )

        return pd.DataFrame(results)

    @staticmethod
    def studyLocusId_based_evidence_table_vs_training_set(
        table_with_score: DataFrame,
        training_set: DataFrame,
        score_column: str,
        min_score: float,
        name_of_the_evidence: str,
    ) -> pd.DataFrame:
        """Stats for stydyLocusId based evidence vs training set.

        Calucalte sensitivity, specificity, PPV, FDR and balanced accuracy
        for tables with studyLocusIds that will allow to combine it with training set directly.

        Args:
            table_with_score (DataFrame): Table with scores to make an evidence and studyLocusId
            training_set (DataFrame): Training set with studyLocusId and goldStandardSet
            score_column (str): Column name with scores in table_with_score
            min_score (float): Minimum score threshold
            name_of_the_evidence (str): Name of the evidence
        Returns:
            pd.DataFrame: List of dataframes and dictionary with stats
        """
        df = (
            table_with_score.withColumn(
                "significant_score",
                f.when(f.col(score_column) >= min_score, 1).otherwise(0),
            )
            .select("studyLocusId", "geneId", "significant_score")
            .join(training_set, on=["studyLocusId", "geneId"], how="right")
            .fillna({"significant_score": 0})
        ).cache()

        TP = df.filter(
            (f.col("significant_score") == 1) & (f.col("goldStandardSet") == "positive")
        ).count()
        TN = df.filter(
            (f.col("significant_score") == 0) & (f.col("goldStandardSet") == "negative")
        ).count()
        FP = df.filter(
            (f.col("significant_score") == 1) & (f.col("goldStandardSet") == "negative")
        ).count()
        FN = df.filter(
            (f.col("significant_score") == 0) & (f.col("goldStandardSet") == "positive")
        ).count()

        results = []

        sensitivity = TP / (TP + FN)
        ppv = TP / (TP + FP)
        specificity = TN / (FP + TN)
        fdr = 1 - ppv
        balanced_accuracy = (sensitivity + specificity) / 2

        results.append(
            {
                "Evidence": name_of_the_evidence,
                "TP": TP,
                "TN": TN,
                "FP": FP,
                "FN": FN,
                "Sensitivity (recall)": sensitivity,
                "Specificity (selectivity)": specificity,
                "PPV (precision)": ppv,
                "FDR": fdr,
                "Balanced_accuracy": balanced_accuracy,
            }
        )

        return pd.DataFrame(results)
