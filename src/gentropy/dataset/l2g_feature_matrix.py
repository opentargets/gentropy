"""Feature matrix of study locus pairs annotated with their functional genomics features."""

from __future__ import annotations

import logging
from functools import reduce
from typing import TYPE_CHECKING, Any, Self

import pyspark.sql.functions as f
from pandas import DataFrame as pd_dataframe
from pyspark.sql import Window

from gentropy.common.spark import convert_from_long_to_wide
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.method.l2g.feature_factory import FeatureFactory, L2GFeatureInputLoader

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from gentropy.dataset.study_locus import StudyLocus


class L2GFeatureMatrix:
    """Dataset with features for Locus to Gene prediction."""

    def __init__(
        self,
        _df: DataFrame,
        features_list: list[str] | None = None,
        with_gold_standard: bool = False,
        label_col: str = "goldStandardSet",
    ) -> None:
        """Post-initialisation to set the features list. If not provided, all columns except the fixed ones are used.

        Args:
            _df (DataFrame): Feature matrix dataset
            features_list (list[str] | None): List of features to use. If None, all possible features are used.
            with_gold_standard (bool): Whether to include the gold standard set in the feature matrix.
            label_col (str): The target column when the feature matrix represents the gold standard

        """
        self.with_gold_standard = with_gold_standard
        self.fixed_cols = ["studyLocusId", "geneId"]
        if self.with_gold_standard:
            self.label_col = label_col
            self.fixed_cols.append(label_col)
        if "traitFromSourceMappedId" in _df.columns:
            self.fixed_cols.append("traitFromSourceMappedId")

        self.features_list = features_list or [
            col for col in _df.columns if col not in self.fixed_cols
        ]
        self._df = _df.selectExpr(
            self.fixed_cols
            + [
                f"CAST({feature} AS FLOAT) AS {feature}"
                for feature in self.features_list
            ]
        )

    @classmethod
    def from_features_list(
        cls: type[L2GFeatureMatrix],
        study_loci_to_annotate: StudyLocus | L2GGoldStandard,
        features_list: list[str],
        features_input_loader: L2GFeatureInputLoader,
    ) -> L2GFeatureMatrix:
        """Generate features from the gentropy datasets by calling the feature factory that will instantiate the corresponding features.

        Args:
            study_loci_to_annotate (StudyLocus | L2GGoldStandard): Study locus pairs to annotate
            features_list (list[str]): List of feature names to be computed.
            features_input_loader (L2GFeatureInputLoader): Object that contais features input.

        Returns:
            L2GFeatureMatrix: L2G feature matrix dataset
        """
        features_long_df = reduce(
            lambda x, y: x.unionByName(y, allowMissingColumns=True),
            [
                # Compute all features and merge them into a single dataframe
                feature.df
                for feature in FeatureFactory(
                    study_loci_to_annotate, features_list
                ).generate_features(features_input_loader)
            ],
        )
        if isinstance(study_loci_to_annotate, L2GGoldStandard):
            return cls(
                _df=convert_from_long_to_wide(
                    # Add gold standard set to the feature matrix
                    features_long_df.join(
                        study_loci_to_annotate.df.select(
                            "studyLocusId", "geneId", "goldStandardSet"
                        ),
                        ["studyLocusId", "geneId"],
                    ),
                    ["studyLocusId", "geneId", "goldStandardSet"],
                    "featureName",
                    "featureValue",
                ),
                with_gold_standard=True,
            )
        return cls(
            _df=convert_from_long_to_wide(
                features_long_df,
                ["studyLocusId", "geneId"],
                "featureName",
                "featureValue",
            ),
            with_gold_standard=False,
        )

    def calculate_feature_missingness_rate(
        self: L2GFeatureMatrix,
    ) -> dict[str, float]:
        """Calculate the proportion of missing values in each feature.

        Returns:
            dict[str, float]: Dictionary of feature names and their missingness rate.

        Raises:
            ValueError: If no features are found.
        """
        total_count = self._df.count()
        if not self.features_list:
            raise ValueError("No features found")

        return {
            feature: (
                self._df.filter(
                    (self._df[feature].isNull()) | (self._df[feature] == 0)
                ).count()
                / total_count
            )
            for feature in self.features_list
        }

    def fill_na(
        self: L2GFeatureMatrix, na_value: float = 0.0, subset: list[str] | None = None
    ) -> L2GFeatureMatrix:
        """Fill missing values in a column with a given value.

        For features that correspond to gene attributes, missing values are imputed using the mean of the column.

        Args:
            na_value (float): Value to replace missing values with. Defaults to 0.0.
            subset (list[str] | None): Subset of columns to consider. Defaults to None.

        Returns:
            L2GFeatureMatrix: L2G feature matrix dataset
        """
        cols_to_impute = [
            "proteinGeneCount500kb",
            "geneCount500kb",
        ]
        for col in cols_to_impute:
            if col not in self._df.columns:
                continue
            else:
                self._df = self._df.withColumn(
                    col,
                    f.when(
                        f.col(col).isNull(),
                        f.mean(f.col(col)).over(Window.partitionBy("studyLocusId")),
                    ).otherwise(f.col(col)),
                )
        self._df = self._df.fillna(na_value, subset=subset)
        return self

    def select_features(
        self: L2GFeatureMatrix,
        features_list: list[str] | None,
    ) -> L2GFeatureMatrix:
        """Returns a new object with a subset of features from the original feature matrix.

        Args:
            features_list (list[str] | None): List of features to select

        Returns:
            L2GFeatureMatrix: L2G feature matrix dataset

        Raises:
            ValueError: If no features have been selected.
        """
        if features_list := features_list or self.features_list:
            # cast to float every feature in the features_list
            return L2GFeatureMatrix(
                _df=self._df.selectExpr(
                    self.fixed_cols
                    + [
                        f"CAST({feature} AS FLOAT) AS {feature}"
                        for feature in features_list
                    ]
                ),
                features_list=features_list,
                with_gold_standard=self.with_gold_standard,
            )
        raise ValueError("features_list cannot be None")

    def persist(self: Self) -> Self:
        """Persist the feature matrix in memory.

        Returns:
            Self: Persisted Dataset
        """
        self._df = self._df.persist()
        return self

    def append_null_features(self, features_list: list[str]) -> L2GFeatureMatrix:
        """Add features from the list that are not already in the dataframe as null columns filled with 0.0.

        Args:
            features_list (list[str]): List of features to check and add if missing

        Returns:
            L2GFeatureMatrix: Updated feature matrix with additional features
        """
        null_features = [
            feature for feature in features_list if feature not in self._df.columns
        ]
        if null_features:
            for feature in null_features:
                self._df = self._df.withColumn(feature, f.lit(0.0))
            self.features_list.extend(null_features)

        return self

    # VEP threshold for coding signal. Set to 0.6 (slightly below the computed
    # score of 0.66 for protein_altering_variant SO:0001818, rank=14/41) to
    # give a small margin around that boundary.
    _VEP_PROTEIN_ALTERING_THRESHOLD: float = 0.6

    # Functional genomics signal features for dark matter detection.
    # A positive with all of these at zero (and vepMaximum below threshold)
    # has no molecular evidence linking it to the locus.
    _DARK_MATTER_SIGNAL_FEATURES: list[str] = [
        "eQtlColocClppMaximum",
        "pQtlColocClppMaximum",
        "sQtlColocClppMaximum",
        "eQtlColocH4Maximum",
        "pQtlColocH4Maximum",
        "sQtlColocH4Maximum",
        "transPQtlColocH4Maximum",
        "e2gMean",
    ]

    # Neighbourhood distance features used to determine whether a gene is the
    # nearest to the sentinel.  A value of 1.0 means the gene is the nearest
    # in the locus window.  A positive that is nearest by any of these measures
    # is excluded from dark matter removal even if it has no functional signal,
    # because its proximity may still make it a learnable positive.
    _DARK_MATTER_NEAREST_FEATURES: list[str] = [
        "distanceSentinelFootprintNeighbourhood",
        "distanceFootprintMeanNeighbourhood",
        "distanceTssMeanNeighbourhood",
        "distanceSentinelTssNeighbourhood",
    ]

    def filter_dark_matter_loci(self: Self) -> tuple[Self, dict[str, Any]]:
        """Remove all loci whose every positive is a dark matter positive.

        A dark matter positive is a gold-standard positive that satisfies both:
        - No functional genomics signal: all QTL colocalisation features and
          e2gMean are zero, and vepMaximum (when present) is below the
          protein-altering threshold (0.6).
        - Not the nearest gene: all neighbourhood distance features are < 1.0.

        A locus is removed only when ALL of its positives are dark matter.
        Loci with at least one signal-carrying or nearest-gene positive are
        kept. The entire locus (positives and negatives) is dropped so class
        balance within retained loci is preserved.

        Returns:
            tuple[Self, dict[str, Any]]: Filtered feature matrix and a stats
                dict with before/after counts and reduction percentages.

        Raises:
            ValueError: If called on a feature matrix without gold standard labels.
        """
        if not self.with_gold_standard:
            raise ValueError(
                "Dark matter filtering requires a gold standard-annotated feature matrix."
            )

        # Only check features actually present in this matrix
        qtl_e2g_features = [
            col
            for col in self._DARK_MATTER_SIGNAL_FEATURES
            if col in self.features_list
        ]
        nearest_features = [
            col
            for col in self._DARK_MATTER_NEAREST_FEATURES
            if col in self.features_list
        ]

        if not qtl_e2g_features and "vepMaximum" not in self.features_list:
            logging.warning(
                "No dark matter signal features found in feature matrix; "
                "filter has no effect."
            )
            return self, {}

        if not nearest_features:
            logging.warning(
                "No neighbourhood distance features found in feature matrix; "
                "cannot determine gene nearness — dark matter filter has no effect."
            )
            return self, {}

        # "No functional signal" condition
        if qtl_e2g_features:
            no_qtl_e2g_signal = reduce(
                lambda acc, col: acc & (f.col(col) == 0.0),
                qtl_e2g_features[1:],
                f.col(qtl_e2g_features[0]) == 0.0,
            )
        else:
            no_qtl_e2g_signal = f.lit(True)

        if "vepMaximum" in self.features_list:
            no_signal = no_qtl_e2g_signal & (
                f.col("vepMaximum") < self._VEP_PROTEIN_ALTERING_THRESHOLD
            )
        else:
            no_signal = no_qtl_e2g_signal

        # "Not the nearest gene" — all neighbourhood distances < 1.0
        not_nearest = reduce(
            lambda acc, col: acc & (f.col(col) < 1.0),
            nearest_features[1:],
            f.col(nearest_features[0]) < 1.0,
        )

        positives = self._df.filter(
            f.col(self.label_col) == L2GGoldStandard.GS_POSITIVE_LABEL
        )

        # Mark a locus for removal only when every positive in it is dark matter.
        total_positives_per_locus = positives.groupBy("studyLocusId").agg(
            f.count("*").alias("total_positive_count")
        )
        dark_matter_positives_per_locus = (
            positives.filter(no_signal & not_nearest)
            .groupBy("studyLocusId")
            .agg(f.count("*").alias("dark_matter_count"))
        )
        dark_matter_loci = (
            total_positives_per_locus.join(
                dark_matter_positives_per_locus, "studyLocusId"
            )
            .filter(f.col("dark_matter_count") == f.col("total_positive_count"))
            .select("studyLocusId")
            .persist()
        )

        # Consolidate before-stats into a single aggregation (3 metrics, 1 action)
        before_row = self._df.agg(
            f.count("*").alias("rows"),
            f.sum(
                f.when(f.col(self.label_col) == L2GGoldStandard.GS_POSITIVE_LABEL, 1).otherwise(0)
            ).alias("positives"),
            f.countDistinct("studyLocusId").alias("loci"),
        ).collect()[0]

        # Dark matter counts: loci removed + matching positive rows (1 action)
        dm_row = dark_matter_loci.join(
            dark_matter_positives_per_locus, "studyLocusId"
        ).agg(
            f.count("*").alias("loci_removed"),
            f.sum("dark_matter_count").alias("dm_positives"),
        ).collect()[0]

        self._df = self._df.join(dark_matter_loci, "studyLocusId", "left_anti")
        dark_matter_loci.unpersist()

        # Consolidate after-stats into a single aggregation (1 action)
        after_row = self._df.agg(
            f.count("*").alias("rows"),
            f.sum(
                f.when(f.col(self.label_col) == L2GGoldStandard.GS_POSITIVE_LABEL, 1).otherwise(0)
            ).alias("positives"),
            f.countDistinct("studyLocusId").alias("loci"),
        ).collect()[0]

        def _pct_reduction(before: int, after: int) -> float:
            return round((before - after) / before * 100, 2) if before else 0.0

        rows_before = int(before_row["rows"])
        positives_before = int(before_row["positives"])
        loci_before = int(before_row["loci"])
        dark_matter_loci_count = int(dm_row["loci_removed"])
        dark_matter_positives_count = int(dm_row["dm_positives"] or 0)
        rows_after = int(after_row["rows"])
        positives_after = int(after_row["positives"])
        loci_after = int(after_row["loci"])

        stats: dict[str, Any] = {
            "before": {
                "rows": rows_before,
                "positives": positives_before,
                "study_locus_ids": loci_before,
            },
            "dark_matter": {
                "positive_rows": dark_matter_positives_count,
                "study_locus_ids_removed": dark_matter_loci_count,
            },
            "after": {
                "rows": rows_after,
                "positives": positives_after,
                "study_locus_ids": loci_after,
            },
            "pct_reduction": {
                "rows": _pct_reduction(rows_before, rows_after),
                "positives": _pct_reduction(positives_before, positives_after),
                "study_locus_ids": _pct_reduction(loci_before, loci_after),
            },
        }

        logging.info(
            "Dark matter filter: removed %d loci (%d dark matter positives). "
            "Training set: %d → %d rows (%.1f%% reduction).",
            dark_matter_loci_count,
            dark_matter_positives_count,
            rows_before,
            rows_after,
            stats["pct_reduction"]["rows"],
        )
        return self, stats

    def generate_train_test_split(
        self,
        test_size: float,
        verbose: bool,
        label_encoder: dict[str, int],
        label_col: str,
    ) -> tuple[pd_dataframe, pd_dataframe]:
        """Generate train and test splits for the feature matrix.

        Args:
            test_size (float): Proportion of the test set
            verbose (bool): Whether to print verbose output
            label_encoder (dict[str, int]): Label encoder for the gold standard set
            label_col (str): Column name for the gold standard set

        Returns:
            tuple[pd_dataframe, pd_dataframe]: Train and test splits
        """
        from gentropy.method.l2g.trainer import LocusToGeneTrainer

        data_df = self._df.toPandas()

        # Encode labels in `goldStandardSet` to a numeric value
        data_df[label_col] = data_df[label_col].map(label_encoder)

        # Generate train, held out sets
        return LocusToGeneTrainer.hierarchical_split(
            data_df, test_size=test_size, verbose=verbose
        )
