"""Step to run a finemapping using."""

from __future__ import annotations

from typing import Any

import numpy as np
import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Row, Window

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
    def susie_finemapper_chr_pos(
        GWAS: SummaryStatistics,
        session: Session,
        study_locus_row: Row,
        study_index: StudyIndex,
        window: int = 1_000_000,
        L: int = 10,
    ) -> StudyLocus:
        """Susie fine-mapper function that uses Summary Statstics, chromosome and position as inputs.

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
        chromosome = study_locus_row.chromosome
        position = study_locus_row.position
        _studyId = study_locus_row.studyId

        study_index_df = study_index._df
        study_index_df = study_index_df.filter(study_index_df.studyId == _studyId)
        _major_population = study_index_df.select(
            "studyId",
            f.array_max(f.col("ldPopulationStructure"))
            .getItem("ldPopulation")
            .alias("majorPopulation"),
        ).collect()[0]["majorPopulation"]

        _region = (
            chromosome
            + ":"
            + str(int(position - window / 2))
            + "-"
            + str(int(position + window / 2))
        )

        GWAS_df = GWAS._df
        GWAS_df = GWAS_df.filter(
            (f.col("studyId") == _studyId)
            & (f.col("chromosome") == chromosome)
            & (f.col("position") >= position - (window / 2))
            & (f.col("position") <= position + (window / 2))
        ).withColumn("z", f.col("beta") / f.col("standardError"))

        ld_index = (
            GnomADLDMatrix()
            .get_locus_index(
                study_locus_row=study_locus_row,
                window_size=window,
                major_population=_major_population,
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
            ld_index, gnomad_ancestry=_major_population
        )

        GWAS_df = GWAS_df.toPandas()
        ld_index = ld_index.toPandas()
        ld_index = ld_index.reset_index()

        # Filtering out the variants that are not in the LD matrix, we don't need them
        df_columns = GWAS_df.columns
        GWAS_df = GWAS_df.merge(ld_index, on="variantId", how="inner")
        GWAS_df = GWAS_df[df_columns].reset_index()

        merged_df = GWAS_df.merge(
            ld_index, left_on="variantId", right_on="variantId", how="inner"
        )
        indices = merged_df["index_y"].values

        ld_to_fm = gnomad_ld[indices][:, indices]
        z_to_fm = GWAS_df["z"].values

        susie_output = SUSIE_inf.susie_inf(z=z_to_fm, LD=ld_to_fm, L=L)
        variant_index = session.spark.createDataFrame(GWAS_df)

        return SusieFineMapperStep.susie_inf_to_studylocus(
            susie_output=susie_output,
            session=session,
            _studyId=_studyId,
            _region=_region,
            variant_index=variant_index,
        )

    @staticmethod
    def susie_inf_to_studylocus(
        susie_output: dict[str, Any],
        session: Session,
        _studyId: str,
        _region: str,
        variant_index: DataFrame,
        cs_lbf_thr: float = 2,
    ) -> StudyLocus:
        """Convert SuSiE-inf output to studyLocus DataFrame.

        Args:
            susie_output (dict[str, Any]): SuSiE-inf output dictionary
            session (Session): Spark session
            _studyId (str): study ID
            _region (str): region
            variant_index (DataFrame): DataFrame with variant information
            cs_lbf_thr (float): credible set logBF threshold, default is 2

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
            filter_row = np.argmax(cumsum_arr >= 0.99)
            if filter_row == 0 and cumsum_arr[0] < 0.99:
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
                        "studyId": f.lit(_studyId),
                        "region": f.lit(_region),
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
