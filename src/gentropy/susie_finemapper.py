"""Step to run a finemapping using."""

from __future__ import annotations

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
