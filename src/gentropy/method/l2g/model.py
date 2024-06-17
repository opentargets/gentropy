"""Locus to Gene classifier."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Type

import skops.io as sio
from pandas import DataFrame as pd_dataframe
from pandas import to_numeric as pd_to_numeric
from sklearn.ensemble import GradientBoostingClassifier
from skops import hub_utils

from gentropy.common.session import Session
from gentropy.common.utils import copy_to_gcs

if TYPE_CHECKING:
    from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
    from gentropy.dataset.l2g_prediction import L2GPrediction


@dataclass
class LocusToGeneModel:
    """Wrapper for the Locus to Gene classifier."""

    model: Any = GradientBoostingClassifier(random_state=42)
    hyperparameters: dict[str, Any] | None = None
    training_data: L2GFeatureMatrix | None = None
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
        cls: Type[LocusToGeneModel], path: str | Path
    ) -> LocusToGeneModel:
        """Load a fitted model from disk.

        Args:
            path (str | Path): Path to the model

        Returns:
            LocusToGeneModel: L2G model loaded from disk

        Raises:
            ValueError: If the model has not been fitted yet
        """
        loaded_model = sio.load(path, trusted=True)
        if not loaded_model._is_fitted():
            raise ValueError("Model has not been fitted yet.")
        return cls(model=loaded_model)

    @classmethod
    def load_from_hub(
        cls: Type[LocusToGeneModel], model_id: str, model_name: str = "classifier.skops"
    ) -> LocusToGeneModel:
        """Load a model from the Hugging Face Hub. This will download the model from the hub and load it from disk.

        Args:
            model_id (str): Model ID on the Hugging Face Hub
            model_name (str): Name of the persisted model to load. Defaults to "classifier.skops".

        Returns:
                LocusToGeneModel: L2G model loaded from the Hugging Face Hub

        Examples:
            >>> LocusToGeneModel.load_from_hub("opentargets/locus_to_gene")
        """
        local_path = Path(model_id)
        hub_utils.download(repo_id=model_id, dst=local_path)
        return cls.load_from_disk(Path(local_path) / model_name)

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

        pd_dataframe.iteritems = pd_dataframe.items

        feature_matrix_pdf = feature_matrix.df.toPandas()
        # L2G score is the probability the classifier assigns to the positive class (the second element in the probability array)
        feature_matrix_pdf["score"] = self.model.predict_proba(  # type: ignore
            # We drop the fixed columns to only pass the feature values to the classifier
            feature_matrix_pdf.drop(feature_matrix.fixed_cols, axis=1)  # type: ignore
            .apply(pd_to_numeric)
            .values  # type: ignore
        )[:, 1]
        output_cols = [field.name for field in L2GPrediction.get_schema().fields]
        return L2GPrediction(
            _df=session.spark.createDataFrame(feature_matrix_pdf.filter(output_cols)),  # type: ignore
            _schema=L2GPrediction.get_schema(),
        )

    def save(self: LocusToGeneModel, path: str) -> None:
        """Saves fitted model to disk using the skops persistence format.

        Args:
            path (str): Path to save the persisted model. Should end with .skops

        Raises:
            ValueError: If the model has not been fitted yet
            ValueError: If the path does not end with .skops
        """
        if self.model is None:
            raise ValueError("Model has not been fitted yet.")
        if not path.endswith(".skops"):
            raise ValueError("Path should end with .skops")
        if path.startswith("gs://"):
            local_path = path.split("/")[-1]
            sio.dump(self.model, local_path)
            copy_to_gcs(local_path, path)
        else:
            sio.dump(self.model, path)

    def _create_hugging_face_model_card(
        self: LocusToGeneModel,
        local_repo: str,
    ) -> None:
        """Create a model card to document the model in the hub. The model card is saved in the local repo before pushing it to the hub.

        Args:
            local_repo (str): Path to the folder where the README file will be saved to be pushed to the Hugging Face Hub
        """
        from skops import card

        # Define card metadata
        description = """The locus-to-gene (L2G) model derives features to prioritise likely causal genes at each GWAS locus based on genetic and functional genomics features. The main categories of predictive features are:

        - Distance: (from credible set variants to gene)
        - Molecular QTL Colocalization
        - Chromatin Interaction: (e.g., promoter-capture Hi-C)
        - Variant Pathogenicity: (from VEP)

        More information at: https://opentargets.github.io/gentropy/python_api/methods/l2g/_l2g/
        """
        how_to = """To use the model, you can load it using the `LocusToGeneModel.load_from_hub` method. This will return a `LocusToGeneModel` object that can be used to make predictions on a feature matrix.
        The model can then be used to make predictions using the `predict` method.

        More information can be found at: https://opentargets.github.io/gentropy/python_api/methods/l2g/model/
        """
        model_card = card.Card(
            self.model,
            metadata=card.metadata_from_config(Path(local_repo)),
        )
        model_card.add(
            **{
                "Model description": description,
                "Model description/Training Procedure": "Gradient Boosting Classifier",
                "How to Get Started with the Model": how_to,
                "Model Card Authors": "Open Targets",
                "License": "MIT",
                "Citation": "https://doi.org/10.1038/s41588-021-00945-5",
            }
        )
        model_card.delete("Model description/Training Procedure/Model Plot")
        model_card.delete("Model description/Evaluation Results")
        model_card.delete("Model Card Authors")
        model_card.delete("Model Card Contact")
        model_card.save(Path(local_repo) / "README.md")

    def export_to_hugging_face_hub(
        self: LocusToGeneModel,
        model_path: str,
        hf_hub_token: str,
        data: pd_dataframe,
        commit_message: str,
        repo_id: str = "opentargets/locus_to_gene",
        local_repo: str = "locus_to_gene",
    ) -> None:
        """Share the model on Hugging Face Hub.

        Args:
            model_path (str): The path to the L2G model file.
            hf_hub_token (str): Hugging Face Hub token
            data (pd_dataframe): Data used to train the model. This is used to have an example input for the model and to store the column order.
            commit_message (str): Commit message for the push
            repo_id (str): The Hugging Face Hub repo id where the model will be stored.
            local_repo (str): Path to the folder where the contents of the model repo + the documentation are located. This is used to push the model to the Hugging Face Hub.

        Raises:
            Exception: If the push to the Hugging Face Hub fails
        """
        from sklearn import __version__ as sklearn_version

        try:
            hub_utils.init(
                model=model_path,
                requirements=[f"scikit-learn={sklearn_version}"],
                dst=local_repo,
                task="tabular-classification",
                data=data,
            )
            self._create_hugging_face_model_card(local_repo)
            hub_utils.push(
                repo_id=repo_id,
                source=local_repo,
                token=hf_hub_token,
                commit_message=commit_message,
                create_remote=True,
            )
        except Exception as e:
            # remove the local repo if the push fails
            if Path(local_repo).exists():
                for p in Path(local_repo).glob("*"):
                    p.unlink()
                Path(local_repo).rmdir()
            raise e
