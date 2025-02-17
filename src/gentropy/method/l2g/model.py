"""Locus to Gene classifier."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
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
    features_list: list[str] = field(
        default_factory=list
    )  # TODO: default to list in config if not provided
    hyperparameters: dict[str, Any] = field(
        default_factory=lambda: {
            "n_estimators": 100,
            "max_depth": 10,
            "ccp_alpha": 0,
            "learning_rate": 0.1,
            "min_samples_leaf": 5,
            "min_samples_split": 5,
            "subsample": 1,
        }
    )
    training_data: L2GFeatureMatrix | None = None
    label_encoder: dict[str, int] = field(
        default_factory=lambda: {
            "negative": 0,
            "positive": 1,
        }
    )

    def __post_init__(self: LocusToGeneModel) -> None:
        """Post-initialisation to fit the estimator with the provided params."""
        self.model.set_params(**self.hyperparameters_dict)

    @classmethod
    def load_from_disk(
        cls: type[LocusToGeneModel], path: str, **kwargs: Any
    ) -> LocusToGeneModel:
        """Load a fitted model from disk.

        Args:
            path (str): Path to the model
            **kwargs(Any): Keyword arguments to pass to the constructor

        Returns:
            LocusToGeneModel: L2G model loaded from disk

        Raises:
            ValueError: If the model has not been fitted yet
        """
        if path.startswith("gs://"):
            path = path.removeprefix("gs://")
            bucket_name = path.split("/")[0]
            blob_name = "/".join(path.split("/")[1:])
            from google.cloud import storage

            client = storage.Client()
            bucket = storage.Bucket(client=client, name=bucket_name)
            blob = storage.Blob(name=blob_name, bucket=bucket)
            data = blob.download_as_string(client=client)
            loaded_model = sio.loads(data, trusted=sio.get_untrusted_types(data=data))
        else:
            loaded_model = sio.load(path, trusted=sio.get_untrusted_types(file=path))

        if not loaded_model._is_fitted():
            raise ValueError("Model has not been fitted yet.")
        return cls(model=loaded_model, **kwargs)

    @classmethod
    def load_from_hub(
        cls: type[LocusToGeneModel],
        model_id: str,
        hf_token: str | None = None,
        model_name: str = "classifier.skops",
    ) -> LocusToGeneModel:
        """Load a model from the Hugging Face Hub. This will download the model from the hub and load it from disk.

        Args:
            model_id (str): Model ID on the Hugging Face Hub
            hf_token (str | None): Hugging Face Hub token to download the model (only required if private)
            model_name (str): Name of the persisted model to load. Defaults to "classifier.skops".

        Returns:
            LocusToGeneModel: L2G model loaded from the Hugging Face Hub
        """

        def get_features_list_from_metadata() -> list[str]:
            """Get the features list (in the right order) from the metadata JSON file downloaded from the Hub.

            Returns:
                list[str]: Features list
            """
            import json

            model_config_path = str(Path(local_path) / "config.json")
            with open(model_config_path) as f:
                model_config = json.load(f)
            return [
                column
                for column in model_config["sklearn"]["columns"]
                if column != "studyLocusId"
            ]

        local_path = Path(model_id)
        hub_utils.download(repo_id=model_id, dst=local_path, token=hf_token)
        features_list = get_features_list_from_metadata()
        return cls.load_from_disk(
            str(Path(local_path) / model_name), features_list=features_list
        )

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
        return self.hyperparameters.default_factory()

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

        feature_matrix_pdf = feature_matrix._df.toPandas()
        # L2G score is the probability the classifier assigns to the positive class (the second element in the probability array)
        feature_matrix_pdf["score"] = self.model.predict_proba(
            # We drop the fixed columns to only pass the feature values to the classifier
            feature_matrix_pdf.drop(feature_matrix.fixed_cols, axis=1)
            .apply(pd_to_numeric)
            .values
        )[:, 1]
        output_cols = [field.name for field in L2GPrediction.get_schema().fields]
        return L2GPrediction(
            _df=session.spark.createDataFrame(feature_matrix_pdf.filter(output_cols)),
            _schema=L2GPrediction.get_schema(),
            model=self,
        )

    def save(self: LocusToGeneModel, path: str) -> None:
        """Saves fitted model to disk using the skops persistence format.

        Args:
            path (str): Path to save the persisted model. Should end with .skops

        Raises:
            ValueError: If the model has not been fitted yet or if the path does not end with .skops
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

    @staticmethod
    def load_feature_matrix_from_wandb(wandb_run_name: str) -> pd.DataFrame:
        """Loads dataset of feature matrix used during a wandb run.

        Args:
            wandb_run_name (str): Name of the wandb run to load the feature matrix from

        Returns:
            pd.DataFrame: Feature matrix used during the wandb run
        """
        with open(wandb_run_name) as f:
            raw_data = json.load(f)

        data = raw_data["data"]
        columns = raw_data["columns"]
        return pd.DataFrame(data, columns=columns)

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
            RuntimeError: If the push to the Hugging Face Hub fails
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
            raise RuntimeError from e
