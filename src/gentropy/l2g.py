"""Steps to build the Locus to Gene feature matrix and to tune the model."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
from xgboost import XGBClassifier

from gentropy.common.session import Session
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.intervals import Intervals
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.pathway_enrichment import PathwayEnrichment
from gentropy.dataset.pathway_index import PathwayIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.target_index import TargetIndex
from gentropy.dataset.variant_index import VariantIndex
from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader
from gentropy.method.l2g.model import LocusToGeneModel
from gentropy.method.l2g.trainer import LocusToGeneTrainer

logger = logging.getLogger(__name__)


class LocusToGeneFeatureMatrixStep:
    """Annotate credible set with functional genomics features."""

    def __init__(
        self,
        session: Session,
        *,
        features_list: list[str],
        credible_set_path: str,
        credible_set_variant_path: str,
        variant_index_path: str | None = None,
        colocalisation_path: str | None = None,
        study_index_path: str | None = None,
        target_index_path: str | None = None,
        intervals_path: str | None = None,
        gene_interactions_path: str | None = None,
        pathway_index_path: str | None = None,
        pathway_enrichment_path: str | None = None,
        feature_matrix_path: str,
        append_null_features: bool = False,
    ) -> None:
        """Initialise the step and run the logic based on mode.

        Args:
            session (Session): Session object that contains the Spark session
            features_list (list[str]): List of features to use for the model
            credible_set_path (str): Path to the credible set dataset necessary to build the feature matrix
            credible_set_variant_path (str): Path to the variants of the credible sets
            variant_index_path (str | None): Path to the variant index dataset
            colocalisation_path (str | None): Path to the colocalisation dataset
            study_index_path (str | None): Path to the study index dataset
            target_index_path (str | None): Path to the target index dataset
            intervals_path (str | None): Path to the interval dataset
            gene_interactions_path (str | None): Path to the protein-protein interaction (PPI) dataset
            pathway_index_path (str | None): Path to the harmonised `PathwayIndex` dataset, as
                written by the pathway ingestion step
            pathway_enrichment_path (str | None): Path to the harmonised `PathwayEnrichment`
                dataset, as written by the pathway ingestion step
            feature_matrix_path (str): Path to the L2G feature matrix output dataset
            append_null_features (bool): Whether to append null features to the feature matrix. Defaults to False.
        """
        credible_set = CredibleSet.from_parquet(
            session, credible_set_path, recursiveFileLookup=True
        ).to_locus_view(
            CredibleSetVariant.from_parquet(
                session, credible_set_variant_path, recursiveFileLookup=True
            )
        )
        studies = (
            StudyIndex.from_parquet(session, study_index_path, recursiveFileLookup=True)
            if study_index_path
            else None
        )
        variant_index = (
            VariantIndex.from_parquet(session, variant_index_path)
            if variant_index_path
            else None
        )
        coloc = (
            Colocalisation.from_parquet(
                session, colocalisation_path, recursiveFileLookup=True
            )
            if colocalisation_path
            else None
        )

        target_index = (
            TargetIndex.from_parquet(
                session, target_index_path, recursiveFileLookup=True
            )
            if target_index_path
            else None
        )

        intervals = (
            Intervals.from_parquet(session, intervals_path, recursiveFileLookup=True)
            if intervals_path
            else None
        )

        interactions = (
            session.load_data(
                gene_interactions_path, "parquet", recursiveFileLookup=True
            )
            if gene_interactions_path
            else None
        )

        pathway_index = (
            PathwayIndex.from_parquet(session, pathway_index_path)
            if pathway_index_path
            else None
        )

        pathway_enrichment = (
            PathwayEnrichment.from_parquet(session, pathway_enrichment_path)
            if pathway_enrichment_path
            else None
        )

        trans_pqtl_features = {
            "transPQtlColocH4Maximum",
            "transPQtlColocH4MaximumNeighbourhood",
        }
        if trans_pqtl_features.intersection(features_list) and interactions is None:
            raise ValueError(
                "Interactions are required for trans-pQTL colocalisation features. "
                "Provide `gene_interactions_path`."
            )
        if trans_pqtl_features.intersection(features_list) and target_index is None:
            raise ValueError(
                "target_index is required for trans-pQTL colocalisation features. "
                "Provide `target_index_path`."
            )

        pathway_features = {
            "pathwayEnrichment500kb",
            "pathwayEnrichment500kbNeighbourhood",
        }
        if pathway_features.intersection(features_list) and (
            pathway_index is None
            or pathway_enrichment is None
            or target_index is None
            or studies is None
        ):
            raise ValueError(
                "Pathway enrichment features need the pathway index, the enrichment "
                "results, the target index and the study index. Provide "
                "`pathway_index_path`, `pathway_enrichment_path`, `target_index_path` "
                "and `study_index_path`."
            )

        features_input_loader = L2GFeatureInputLoader(
            variant_index=variant_index,
            colocalisation=coloc,
            study_index=studies,
            study_locus=credible_set,
            target_index=target_index,
            intervals=intervals,
            interactions=interactions,
            pathway_index=pathway_index,
            pathway_enrichment=pathway_enrichment,
        )

        fm = credible_set.filter(f.col("studyType") == "gwas").build_feature_matrix(
            features_list,
            features_input_loader,
            append_null_features=append_null_features,
        )

        if target_index is not None:
            target_index_df = target_index.df.select("id", "biotype").withColumnRenamed(
                "id", "geneId"
            )

            target_index_df = target_index_df.withColumn(
                "isProteinCoding",
                f.when(f.col("biotype") == "protein_coding", 1).otherwise(0),
            ).drop("biotype")

            fm._df = fm._df.drop("isProteinCoding").join(
                target_index_df, on="geneId", how="inner"
            )

        fm._df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(feature_matrix_path)


class LocusToGeneModelTuningStep:
    """Run L2G cross-validation on pre-built, annotated train and test feature matrices.

    This step does not construct the training set or perform
    any gold-standard parsing. It expects parquet files that already contain the
    feature columns and the ``goldStandardSet`` label column.
    """

    def __init__(
        self,
        session: Session,
        *,
        train_feature_matrix_path: str,
        test_feature_matrix_path: str,
        hyperparameters: dict[str, Any],
        features_list: list[str] | None = None,
        n_splits: int = 5,
        hyperparameter_grid: dict[str, Any] | None = None,
        cv_results_dir: str | None = None,
        holdout_only: bool = False,
    ) -> None:
        """Initialise the step and run cross-validation.

        Args:
            session (Session): Session object that contains the Spark session
            train_feature_matrix_path (str): Path to the parquet file containing the
                annotated training feature matrix.  Must include ``credibleSetId``,
                ``geneId``, a ``goldStandardSet`` column with values ``"positive"``/
                ``"negative"`` (strings) or ``1``/``0`` (integers), and one column per feature.
            test_feature_matrix_path (str): Path to the parquet file containing the
                annotated test feature matrix with the same schema as the training file.
            hyperparameters (dict[str, Any]): Hyperparameters passed to XGBClassifier.
            features_list (list[str] | None): Subset of feature columns to use.  When
                ``None`` every column that is not a fixed metadata column is used.
            n_splits (int): Number of cross-validation folds. Defaults to 5.
            hyperparameter_grid (dict[str, Any] | None): Grid to sweep over in the form
                ``{"param": {"values": [v1, v2, ...]}, ...}``.  When provided with
                ``cv_results_dir``, all configs are evaluated and written to file.
                Defaults to None (CV uses the fixed ``hyperparameters``).
            cv_results_dir (str | None): Directory (local or ``gs://``) to write
                ``cv_results.json``, ``cv_folds.csv``, and per-config plots.  When None,
                metrics are logged to the terminal. Defaults to None.
            holdout_only (bool): When True, skip CV folds and evaluate each config directly
                on the holdout set. Requires ``cv_results_dir`` and a non-empty test set.
                Defaults to False.
        """
        train_feature_matrix = L2GFeatureMatrix(
            _df=session.load_data(train_feature_matrix_path, "parquet"),
            features_list=features_list,
            with_gold_standard=True,
        ).persist()

        test_feature_matrix = L2GFeatureMatrix(
            _df=session.load_data(test_feature_matrix_path, "parquet"),
            features_list=features_list,
            with_gold_standard=True,
        ).persist()

        effective_features = train_feature_matrix.features_list

        l2g_model = LocusToGeneModel(
            model=XGBClassifier(random_state=777, eval_metric="aucpr"),
            hyperparameters=hyperparameters,
            features_list=effective_features,
        )

        trainer = LocusToGeneTrainer(
            model=l2g_model,
            feature_matrix=train_feature_matrix,
        )

        # Convert to pandas and encode labels, mirroring generate_train_test_split
        label_col = train_feature_matrix.label_col
        train_pd: pd.DataFrame = train_feature_matrix._df.toPandas()
        test_pd: pd.DataFrame = test_feature_matrix._df.toPandas()
        train_feature_matrix._df.unpersist()
        test_feature_matrix._df.unpersist()
        # The split step saves goldStandardSet as integers 0/1; only encode if still strings.
        for df in (train_pd, test_pd):
            if df[label_col].dtype == object:
                df[label_col] = df[label_col].map(l2g_model.label_encoder)

        x_train: np.ndarray = (
            train_pd[effective_features].apply(pd.to_numeric).to_numpy()
        )
        y_train: np.ndarray = train_pd[label_col].apply(pd.to_numeric).to_numpy()
        x_test: np.ndarray = (
            test_pd[effective_features].apply(pd.to_numeric).to_numpy()
        )
        y_test: np.ndarray = test_pd[label_col].apply(pd.to_numeric).to_numpy()

        trainer.train_df = train_pd
        trainer.test_df = test_pd
        trainer.x_train = x_train
        trainer.y_train = y_train
        trainer.x_test = x_test
        trainer.y_test = y_test

        trainer.cross_validate(
            parameter_grid=hyperparameter_grid,
            n_splits=n_splits,
            cv_results_dir=cv_results_dir,
            holdout_only=holdout_only,
        )

        trainer.fit()

        trainer.log_to_terminal(
            eval_id="Hold-out",
            metrics=trainer.evaluate(
                y_true=y_test,
                y_pred=trainer.model.model.predict(x_test),
                y_pred_proba=trainer.model.model.predict_proba(x_test),
            ),
        )
