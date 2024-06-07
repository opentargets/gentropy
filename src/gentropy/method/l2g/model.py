"""Locus to Gene classifier."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Type

import skops.io as sio
from pandas import to_numeric as pd_to_numeric
from sklearn.ensemble import GradientBoostingClassifier

from gentropy.common.session import Session
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix

if TYPE_CHECKING:
    from gentropy.dataset.l2g_prediction import L2GPrediction


@dataclass
class LocusToGeneModel:
    """Wrapper for the Locus to Gene classifier."""

    features_list: list[str]
    model: Any = GradientBoostingClassifier(random_state=42)
    hyperparameters: dict[str, Any] | None = None
    label_encoder: dict[str, int] = field(
        default_factory=lambda: {
            "negative": 0,
            "positive": 1,
        }
    )

    def __post_init__(self: LocusToGeneModel) -> None:
        """Post-initialisation to fit the estimator with the provided params."""
        if self.hyperparameters:
            self.model.set_params(**self.hyperparameters_dict)

    @classmethod
    def load_from_disk(
        cls: Type[LocusToGeneModel], path: str, features_list: list[str]
    ) -> LocusToGeneModel:
        """Load a fitted model from disk.

        Args:
            path (str): Path to the model
            features_list (list[str]): List of features used for the model

        Returns:
            LocusToGeneModel: L2G model loaded from disk

        Raises:
            ValueError: If the model has not been fitted yet
        """
        loaded_model = sio.load(path, trusted=True)
        if not loaded_model._is_fitted():
            raise ValueError("Model has not been fitted yet.")
        return cls(model=loaded_model, features_list=features_list)

    @property
    def hyperparameters_dict(self) -> dict[str, Any]:
        """Return hyperparameters as a dictionary.

        Returns:
            dict[str, Any]: Hyperparameters

        Raises:
            ValueError: If hyperparameters have not been set
        """
        if not self.hyperparameters:
            raise ValueError("Hyperparameters have not been set.")
        elif isinstance(self.hyperparameters, dict):
            return self.hyperparameters
        return self.hyperparameters.default_factory()  # type: ignore

    def get_feature_importance(self: LocusToGeneModel) -> dict[str, float]:
        """Return dictionary with relative importances of every feature in the model. Feature names are encoded and have to be mapped back to their original names.

        Returns:
            dict[str, float]: Dictionary mapping feature names to their importance
        """
        importances = self.model.feature_importances
        return dict(zip(self.features_list, importances))

    def predict(
        self: LocusToGeneModel,
        feature_matrix: L2GFeatureMatrix,
        session: Session,
    ) -> L2GPrediction:
        """Apply the model to a given feature matrix dataframe. The feature matrix needs to be preprocessed first.

        Args:
            feature_matrix (L2GFeatureMatrix): Feature matrix to apply the model to.
            session (Session): Session object to convert data to Spark

        Returns:
            L2GPrediction: Dataset containing credible sets and their L2G scores
        """
        from gentropy.dataset.l2g_prediction import L2GPrediction

        predictions_df = feature_matrix.df.toPandas().apply(pd_to_numeric)  # type: ignore
        # L2G score is the probability the classifier assigns to the positive class (the second element in the probability array)
        predictions_df["score"] = self.model.predict_proba(
            predictions_df[self.features_list].values
        )[:, 1]  # type: ignore
        return L2GPrediction(
            _df=session.spark.createDataFrame(predictions_df).select(
                "studyLocusId", "geneId", "score"
            ),
            _schema=L2GPrediction.get_schema(),
        )

    def save(self: LocusToGeneModel, path: str) -> None:
        """Saves fitted model to disk using the skops persistence format.

        Args:
            path (str): Path to save the model to

        Raises:
            ValueError: If the model has not been fitted yet
        """
        if self.model is None:
            raise ValueError("Model has not been fitted yet.")
        sio.dump(self.model, path)
