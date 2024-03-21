"""Step to run a finemapping using."""

from __future__ import annotations

from typing import Any

import numpy as np
import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window

from gentropy.common.session import Session
from gentropy.dataset.study_locus import StudyLocus


class SusieFineMapperStep:
    """SuSie finemaping. It has generic methods to run SuSie fine mapping for a study locus."""

    @staticmethod
    def susie_inf_to_studylocus(
        susie_output: dict[str, Any],
        session: Session,
        _studyId: str,
        _region: str,
        _join: DataFrame,
        cs_lbf_thr: float = 2,
    ) -> StudyLocus:
        """Convert SuSiE-inf output to studyLocus DataFrame.

        Args:
            susie_output (dict[str, Any]): SuSiE-inf output dictionary
            session (Session): Spark session
            _studyId (str): study ID
            _region (str): region
            _join (DataFrame): DataFrame with variant information
            cs_lbf_thr (float): credible set logBF threshold, default is 2

        Returns:
            StudyLocus: StudyLocus object with fine-mapped credible sets
        """
        variants = np.array(
            [row["variantId"] for row in _join.select("variantId").collect()]
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
        for i, value in order_creds:
            if counter > 0 and value < cs_lbf_thr:
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
                    _join.select(
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
                        "credibleSetlog10BF": f.lit(value * 0.4342944819),
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
                cred_sets = cred_sets.union(cred_set)
        return StudyLocus(
            _df=cred_sets,
            _schema=StudyLocus.get_schema(),
        )
