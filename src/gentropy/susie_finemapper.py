"""Step to run a finemapping using."""

from __future__ import annotations

from typing import Any

import numpy as np
import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.susie_inf import SUSIE_inf


class SusieFineMapperStep:
    """SuSie finemaping. It has generic methods to run SuSie fine mapping for a study locus.

    This class/step is the temporary solution of the fine-mapping warpper for the development purposes.
    In the future this step will be refactored and moved to the methods module.
    """

    @staticmethod
    def susie_finemapper_one_locus(
        GWAS: SummaryStatistics,
        session: Session,
        study_locus: StudyLocus,
        study_index: StudyIndex,
        window: int = 1_000_000,
    ) -> StudyLocus:
        """Susie fine-mapper for one study locus.

        Args:
            GWAS (SummaryStatistics): GWAS summary statistics
            session (Session): Spark session
            study_locus (StudyLocus): StudyLocus object with one row (the first one will be used)
            study_index (StudyIndex): StudyIndex object
            window (int): window size for fine-mapping

        Returns:
            StudyLocus: StudyLocus object with fine-mapped credible sets
        """
        study_locus_df = study_locus._df
        first_line = study_locus_df.first()
        chromosome = first_line.chromosome
        position = first_line.position
        _studyId = first_line.studyId

        study_index_df = study_index._df
        study_index_df = study_index_df.filter(study_index_df.studyId == _studyId)
        _major_population = study_index_df.select(
            "studyId",
            f.array_max(f.col("ldPopulationStructure"))
            .getItem("ldPopulation")
            .alias("majorPopulation"),
        )

        _region = (
            first_line.withColumn(
                "region",
                f.regexp_replace(
                    f.concat(
                        f.col("chromosome"),
                        f.lit(":"),
                        f.format_number((f.col("position") - (window / 2)), 0),
                        f.lit("-"),
                        f.format_number((f.col("position") + (window / 2)), 0),
                    ),
                    ",",
                    "",
                ),
            )
            .select("region")
            .collect()[0]
        )

        GWAS_df = GWAS._df
        GWAS_df = GWAS_df.filter(
            (f.col("chromosome") == chromosome)
            & (f.col("position") >= position - (window / 2))
            & (f.col("position") <= position + (window / 2))
        ).withColumn("z", f.col("beta") / f.col("standardError"))

        _z = np.array([row["z"] for row in GWAS_df.select("z").collect()])

        # # Extract summary statistics
        # _ss = (
        #     SummaryStatistics.get_locus_sumstats(session, window, _locus)
        #     .withColumn("z", f.col("beta") / f.col("standardError"))
        #     .withColumn("ref", f.split(f.col("variantId"), "_").getItem(2))
        #     .withColumn("alt", f.split(f.col("variantId"), "_").getItem(3))
        #     .select(
        #         "variantId",
        #         f.col("chromosome").alias("chr"),
        #         f.col("position").alias("pos"),
        #         "ref",
        #         "alt",
        #         "beta",
        #         "pValueMantissa",
        #         "pValueExponent",
        #         "effectAlleleFrequencyFromSource",
        #         "standardError",
        #         "z",
        #     )
        # )

        # Extract LD index
        # _index = GnomADLDMatrix.get_locus_index(
        #     session, locus, window_size=window, major_population=_major_population
        # )
        # _join = (
        #     _ss.join(
        #         _index.alias("_index"),
        #         on=(
        #             (_ss["chr"] == _index["chromosome"])
        #             & (_ss["pos"] == _index["position"])
        #             & (_ss["ref"] == _index["referenceAllele"])
        #             & (_ss["alt"] == _index["alternateAllele"])
        #         ),
        #     )
        #     .drop("ref", "alt", "chr", "pos")
        #     .sort("idx")
        # )

        # Extracting z-scores and LD matrix, then running SuSiE-inf
        # _ld = GnomADLDMatrix.get_locus_matrix(_join, gnomad_ancestry=_major_population)

        _ld = 1
        susie_output = SUSIE_inf.susie_inf(z=_z, LD=_ld, L=10)

        return SusieFineMapperStep.susie_inf_to_studylocus(
            susie_output=susie_output,
            session=session,
            _studyId=_studyId,
            _region=_region,
            variant_index=GWAS,
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
