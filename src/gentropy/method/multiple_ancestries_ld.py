"""The combination of multiple ancestries for LD matrix."""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.functions import array, map_from_arrays

from gentropy.common.session import Session
from gentropy.datasource.gnomad.ld import GnomADLDMatrix


class multipleAncestriesLD:
    """Basic functons for multiple ancestries combination."""

    @staticmethod
    def combineLD(
        LD: dict[str, np.ndarray],
        m_p: np.ndarray,
        alphas: list[float],
        pops: list[str],
        zero_af_substitution: float = 1e-4,
    ) -> dict[str, Any]:
        """Combine LD matrices from multiple ancestries.

        Args:
            LD (dict[str, np.ndarray]): the dictionarry of LD matricies, with dimensions, the keys are the ancestries.
            m_p (np.ndarray): matrix of SNP alellic frequncies with dimensions [n_nsps, n_pops].
            alphas (list[float]): list of coefficints that are population poroprtion; the sum of alphas should be 1.
            pops (list[str]): list of ancestries in the same order as in alphas and in m_p.
            zero_af_substitution (float, optional): the value to substitute for zero allele frequencies. Defaults to 1e-4.

        Returns:
            dict[str, Any]: A dictionary containing the combined LD matrix, varG matrix, and frequency.

        Examples:
            >>> corr_matrix_1 = np.array([
            ...     [1.0, 0.5, 0.3, 0.2],
            ...     [0.5, 1.0, 0.4, 0.3],
            ...     [0.3, 0.4, 1.0, 0.5],
            ...     [0.2, 0.3, 0.5, 1.0]
            ... ])
            >>> corr_matrix_2 = np.array([
            ...     [1.0, 0.6, 0.4, 0.3],
            ...     [0.6, 1.0, 0.5, 0.4],
            ...     [0.4, 0.5, 1.0, 0.6],
            ...     [0.3, 0.4, 0.6, 1.0]
            ... ])
            >>> corr_matrix_3 = np.array([
            ...     [1.0, 0.3, 0.2, 0.1],
            ...     [0.3, 1.0, 0.4, 0.2],
            ...     [0.2, 0.4, 1.0, 0.5],
            ...     [0.1, 0.2, 0.5, 1.0]
            ... ])
            >>> LD = {"pop1": corr_matrix_1, "pop2": corr_matrix_2, "pop3": corr_matrix_3}
            >>> m_p = np.array([
            ...     [0.1, 0.2, 0.3],
            ...     [0.4, 0.5, 0.6],
            ...     [0.7, 0.8, 0.9],
            ...     [0.1, 0.3, 0.5]
            ... ])
            >>> pops = ["pop1", "pop2", "pop3"]
            >>> alphas = [0.4, 0.2, 0.4]
            >>> result = multipleAncestriesLD.combineLD(LD=LD, m_p=m_p, alphas=alphas, pops=pops)
            >>> expected_result = {
            ...     'LD': np.array([
            ...         [1.        , 0.46271571, 0.33509199, 0.2944123 ],
            ...         [0.46271571, 1.        , 0.45839856, 0.35042965],
            ...         [0.33509199, 0.45839856, 1.        , 0.55294339],
            ...         [0.2944123 , 0.35042965, 0.55294339, 1.        ]
            ...     ]),
            ...     'varg': np.array([0.336, 0.516, 0.336, 0.484]),
            ...     'freq': np.array([0.2, 0.5, 0.8, 0.3])
            ... }
            >>> assert np.allclose(result['LD'], expected_result['LD']), "LD matrices are not close enough"
            >>> assert np.allclose(result['varg'], expected_result['varg']), "varg arrays are not close enough"
            >>> assert np.allclose(result['freq'], expected_result['freq']), "Frequency arrays are not close enough"
        """
        m_p[m_p == 0] = zero_af_substitution
        m_p[m_p == 1] = 1 - zero_af_substitution

        n_pops = len(pops)
        alphas = np.array(alphas)
        varG = 2 * m_p * (1 - m_p)

        EXY = []
        for i in range(n_pops):
            pop = pops[i]
            varg = varG[:, i]
            vv = np.sqrt(np.outer(varg, varg))
            exey = 4 * np.outer(m_p[:, i], m_p[:, i])
            exy = LD[pop] * vv + exey
            EXY.append(exy)

        freq = np.apply_along_axis(lambda x: np.sum(alphas * x), axis=1, arr=m_p)
        EXEY = 4 * np.outer(freq, freq)

        EXY_t = alphas[0] * EXY[0]
        for i in range(1, n_pops):
            EXY_t += alphas[i] * EXY[i]

        covxy = EXY_t - EXEY
        varg = np.diag(covxy)
        new_ld = covxy * np.sqrt(1 / np.outer(varg, varg))

        return {"LD": new_ld, "varg": varg, "freq": freq}

    @staticmethod
    def prepare_data_frames(
        session: Session,
        chromosomeInput: str,
        start: int,
        end: int,
        path_to_ld_index: str,
        listOfAncestries: list[str] = ["nfe", "afr"],
        full_list_of_ancestries: list[str] = [
            "oth",
            "amr",
            "fin",
            "ami",
            "mid",
            "nfe",
            "sas",
            "asj",
            "eas",
            "afr",
        ],
    ) -> dict[str, Any]:
        """Extract the AFs and LD matrices for the given chromosome and region and the list of ancestires.

        Args:
            session (Session): The pyspark session object.
            chromosomeInput (str): Chromosome number.
            start (int): Start position of the region.
            end (int): End position of the region.
            path_to_ld_index (str): Path to the LD index file with AFs.
            listOfAncestries (list[str], optional): List of ancestries. Defaults to ["nfe","afr"].
            full_list_of_ancestries (list[str], optional): List of all ancestries. Defaults to ['oth', 'amr', 'fin', 'ami', 'mid', 'nfe', 'sas', 'asj', 'eas', 'afr'].

        Returns:
            dict[str, Any]: A dictionary containing the LD and ref_af.
        """
        # PLEASE DO NOT REMOVE THIS LINE
        pd.DataFrame.iteritems = pd.DataFrame.items

        init_df = GnomADLDMatrix().get_locus_index_boundaries_no_studyLocus(
            chromosomeInput, start, end, major_population=listOfAncestries[0]
        )
        init_df = (
            init_df.withColumn(
                "variantId",
                f.concat(
                    f.lit(chromosomeInput),
                    f.lit("_"),
                    f.col("`locus.position`"),
                    f.lit("_"),
                    f.col("alleles")[0],
                    f.lit("_"),
                    f.col("alleles")[1],
                ),
            )
            .select("variantId")
            .distinct()
        )
        for ancestry in listOfAncestries[1:]:
            df = GnomADLDMatrix().get_locus_index_boundaries_no_studyLocus(
                chromosomeInput, start, end, major_population=ancestry
            )
            df = (
                df.withColumn(
                    "variantId",
                    f.concat(
                        f.lit(chromosomeInput),
                        f.lit("_"),
                        f.col("`locus.position`"),
                        f.lit("_"),
                        f.col("alleles")[0],
                        f.lit("_"),
                        f.col("alleles")[1],
                    ),
                )
                .select("variantId")
                .distinct()
            )
            init_df = (
                init_df.join(df, init_df["variantId"] == df["variantId"], how="inner")
                .drop(df["variantId"])
                .distinct()
            )

        ldIndex = session.spark.read.parquet(path_to_ld_index)
        df = ldIndex.withColumn(
            "keys",
            array(
                [
                    f.col("alleleFrequencies")[i]["populationName"]
                    for i in range(len(full_list_of_ancestries))
                ]
            ),
        )
        df = df.withColumn(
            "values",
            array(
                [
                    f.col("alleleFrequencies")[i]["alleleFrequency"]
                    for i in range(len(full_list_of_ancestries))
                ]
            ),
        )
        df = df.withColumn("alleleFrequencies", map_from_arrays(df.keys, df.values))
        ldIndex = df.select(
            "*",
            *[f.col("alleleFrequencies")[key].alias(key) for key in listOfAncestries],
        ).select(["variantId"] + listOfAncestries)

        ref_af = (
            init_df.join(
                ldIndex, init_df["variantId"] == ldIndex["variantId"], how="inner"
            )
            .drop(ldIndex["variantId"])
            .distinct()
        )
        ref_af = ref_af.toPandas()
        ref_af = ref_af.drop_duplicates(subset="variantId")

        LD = {}
        for ancestry in listOfAncestries:
            df = GnomADLDMatrix().get_locus_index_boundaries_no_studyLocus(
                chromosomeInput, start, end, major_population=ancestry
            )
            df = df.withColumn(
                "variantId",
                f.concat(
                    f.lit(chromosomeInput),
                    f.lit("_"),
                    f.col("`locus.position`"),
                    f.lit("_"),
                    f.col("alleles")[0],
                    f.lit("_"),
                    f.col("alleles")[1],
                ),
            )
            schema_df = df.schema
            df = df.toPandas()
            df = df.drop_duplicates(subset="variantId")
            matched_df = pd.merge(ref_af, df, on="variantId", how="inner")
            df = matched_df[df.columns]
            df = df.sort_values(by="idx")
            df_spark = session.spark.createDataFrame(df, schema=schema_df)
            gnomad_ld = GnomADLDMatrix.get_numpy_matrix(
                df_spark, gnomad_ancestry=ancestry
            )

            df["index_column"] = df.reset_index(drop=True).index
            df_ordered = ref_af[["variantId"]].merge(df, on="variantId", how="left")
            index = df_ordered["index_column"].to_list()
            gnomad_ld = gnomad_ld[index, :][:, index]

            LD[ancestry] = gnomad_ld

        return {"LD": LD, "ref_af": ref_af}
