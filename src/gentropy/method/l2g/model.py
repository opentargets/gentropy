"""Locus to Gene classifier."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
import skops.io as sio
from huggingface_hub import HfApi, ModelCard, ModelCardData, create_repo
from pandas import DataFrame as pd_dataframe
from pandas import to_numeric as pd_to_numeric
from sklearn.ensemble import GradientBoostingClassifier
from xgboost import XGBClassifier

from gentropy.common.session import Session
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.external.gcs import copy_to_gcs

if TYPE_CHECKING:
    from gentropy.dataset.l2g_prediction import L2GPrediction


@dataclass
class LocusToGeneModel:
    """Wrapper for the Locus to Gene classifier."""

    model: Any = XGBClassifier(random_state=42, eval_metric="aucpr")
    features_list: list[str] = field(default_factory=list)
    hyperparameters: dict[str, Any] = field(
        default_factory=lambda: {
            "max_depth": 5,
            "reg_alpha": 1,  # L1 regularization
            "reg_lambda": 1.0,  # L2 regularization
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "eta": 0.05,
            "min_child_weight": 10,
            "scale_pos_weight": 0.8,
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
        cls: type[LocusToGeneModel],
        session: Session,
        path: str,
        model_name: str = "classifier.skops",
        **kwargs: Any,
    ) -> LocusToGeneModel:
        """Load a fitted model from disk.

        Args:
            session (Session): Session object that loads the training data
            path (str): Path to the directory containing model and metadata
            model_name (str): Name of the persisted model to load. Defaults to "classifier.skops".
            **kwargs(Any): Keyword arguments to pass to the constructor

        Returns:
            LocusToGeneModel: L2G model loaded from disk

        Raises:
            ValueError: If the model has not been fitted yet
        """
        model_path = (Path(path) / model_name).as_posix()
        if model_path.startswith("gs://"):
            path = model_path.removeprefix("gs://")
            bucket_name = path.split("/")[0]
            blob_name = "/".join(path.split("/")[1:])
            from google.cloud import storage

            client = storage.Client()
            bucket = storage.Bucket(client=client, name=bucket_name)
            blob = storage.Blob(name=blob_name, bucket=bucket)
            data = blob.download_as_string(client=client)
            loaded_model = sio.loads(data, trusted=sio.get_untrusted_types(data=data))
        else:
            loaded_model = sio.load(
                model_path, trusted=sio.get_untrusted_types(file=model_path)
            )
            try:
                # Try loading the training data if it is in the model directory
                training_data = L2GFeatureMatrix(
                    _df=session.spark.createDataFrame(
                        # Parquets are read with Pandas to easily read local files
                        pd.concat(
                            [
                                pd.read_parquet(
                                    (Path(path) / "train.parquet").as_posix()
                                ),
                                pd.read_parquet(
                                    (Path(path) / "test.parquet").as_posix()
                                ),
                            ]
                        )
                    ),
                    features_list=kwargs.get("features_list"),
                )
            except Exception as e:
                logging.error("Training data set to none. Error: %s", e)
                training_data = None

        if (
            isinstance(loaded_model, GradientBoostingClassifier)
            and not loaded_model._is_fitted()
        ) or (
            isinstance(loaded_model, XGBClassifier) and not loaded_model.get_booster()
        ):
            raise ValueError("Model has not been fitted yet.")
        return cls(model=loaded_model, training_data=training_data, **kwargs)

    @classmethod
    def load_from_hub(
        cls: type[LocusToGeneModel],
        session: Session,
        hf_model_id: str,
        hf_model_version: str | None = None,
        hf_token: str | None = None,
    ) -> LocusToGeneModel:
        """Load a model from the Hugging Face Hub. This will download the model from the hub and load it from disk.

        Args:
            session (Session): Session object to load the training data
            hf_model_id (str): Model ID on the Hugging Face Hub
            hf_model_version (str | None): Tag, branch, or commit hash to download the model from the Hub. If None, the latest commit is downloaded.
            hf_token (str | None): Hugging Face Hub token to download the model (only required if private)

        Returns:
            LocusToGeneModel: L2G model loaded from the Hugging Face Hub
        """
        from huggingface_hub import snapshot_download

        def get_features_list_from_metadata() -> list[str]:
            """Get the features list (in the right order) from the metadata JSON file downloaded from the Hub.

            Returns:
                list[str]: Features list
            """
            model_config_path = str(Path(local_path) / "config.json")
            with open(model_config_path) as f:
                model_config = json.load(f)
            return [
                column
                for column in model_config["sklearn"]["columns"]
                if column
                not in [
                    "studyLocusId",
                    "geneId",
                    "traitFromSourceMappedId",
                    "goldStandardSet",
                ]
            ]

        local_path = snapshot_download(
            repo_id=hf_model_id,
            local_dir=hf_model_id,
            revision=hf_model_version,
            token=hf_token,
        )
        features_list = get_features_list_from_metadata()
        return cls.load_from_disk(
            session,
            local_path,
            features_list=features_list,
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
            # create directory if path does not exist
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            sio.dump(self.model, path)

    def _create_hugging_face_model_card(
        self: LocusToGeneModel,
        local_repo: str,
    ) -> None:
        """Create a model card to document the model in the hub. The model card is saved in the local repo before pushing it to the hub.

        Args:
            local_repo (str): Path to the folder where the README file will be saved to be pushed to the Hugging Face Hub
        """
        card_data = ModelCardData(
            language="en",
            license="mit",
            library_name="sklearn",
            tags=[
                "sklearn",
                "tabular-classification",
                "genomics",
                "gwas",
                "gene-prioritization",
            ],
        )

        # Create model card with custom template
        card = ModelCard.from_template(
            card_data,
            template_path=None,
        )

        card.text = """# Locus-to-Gene (L2G) Model

The locus-to-gene (L2G) model prioritises likely causal genes at each GWAS locus based on genetic and functional genomics features.

## Model Description

This is a **Gradient Boosting Classifier** (XGBoost) trained to predict causal genes at GWAS loci.

Limited to protein-coding genes with available feature data.

**Key Features:**
- **Distance**: proximity from credible set variants to gene
- **Molecular QTL Colocalization**: evidence from expression/protein QTL studies
- **Variant Pathogenicity**: VEP (Variant Effect Predictor) scores

## Usage

```python
from gentropy.method.l2g.model import LocusToGeneModel
from gentropy.common.session import Session

# Load model from Hugging Face Hub
session = Session()
model = LocusToGeneModel.load_from_hub(
    session=session,
    hf_model_id="opentargets/locus_to_gene"
)

# Make predictions on your L2G feature matrix
predictions = model.predict(your_feature_matrix, session)
```

## Training

- **Architecture**: XGBoost Gradient Boosting Classifier
- **Training Data**: Curated positive/negative gene-locus pairs from Open Targets
- **Evaluation Metric**: Area under precision-recall curve (AUCPR)

## Citation

If you use this model, please cite:

```bibtex
@article{ghoussaini2021open,
title={Open Targets Genetics: systematic identification of trait-associated genes using large-scale genetics and functional genomics},
author={Ghoussaini, Maya and Mountjoy, Edward and Carmona, Maria and others},
journal={Nature Genetics},
volume={53},
pages={1527--1533},
year={2021},
doi={10.1038/s41588-021-00945-5}
}
```

## More Information

- **Repository**: [opentargets/gentropy](https://github.com/opentargets/gentropy)
- **Documentation**: [L2G Method Docs](https://opentargets.github.io/gentropy/python_api/methods/l2g/_l2g/)
- **Developer**: Open Targets
"""
        card.save(Path(local_repo) / "README.md")

    def export_to_hugging_face_hub(
        self: LocusToGeneModel,
        model_path: str,
        hf_hub_token: str,
        feature_matrix: L2GFeatureMatrix,
        commit_message: str,
        repo_id: str = "opentargets/locus_to_gene",
        test_size: float = 0.15,
    ) -> None:
        """Share the model and training dataset on Hugging Face Hub.

        This will save both the trained model and the train/test splits used for
        training to enable full reproducibility.

        Args:
            model_path (str): The path to the L2G model file.
            hf_hub_token (str): Hugging Face Hub token
            feature_matrix (L2GFeatureMatrix): Data used to train the model. This is used to have an example input for the model and to store the column order.
            commit_message (str): Commit message for the push
            repo_id (str): The Hugging Face Hub repo id where the model will be stored.
            test_size (float): Proportion of data to include in the test split. Defaults to 0.15

        Raises:
            RuntimeError: If the push to the Hugging Face Hub fails
        """
        import shutil
        import tempfile

        from sklearn import __version__ as sklearn_version

        # Create a temporary directory for all operations
        with tempfile.TemporaryDirectory(prefix="l2g_hf_hub_") as temp_dir:
            temp_dir_path = Path(temp_dir)

            try:
                # Create train/test split
                train_df, test_df = feature_matrix.generate_train_test_split(
                    test_size=test_size,
                    verbose=True,
                    label_encoder=self.label_encoder,
                    label_col=feature_matrix.label_col,
                )
                train_df.to_parquet(temp_dir_path / "train.parquet")
                test_df.to_parquet(temp_dir_path / "test.parquet")

                shutil.copy(model_path, temp_dir_path / "classifier.skops")

                with open(temp_dir_path / "config.json", "w") as f:
                    config = {
                        "sklearn": {
                            "columns": train_df.columns.tolist(),
                            "sklearn_version": sklearn_version,
                        },
                        "task": "tabular-classification",
                    }
                    json.dump(config, f, indent=2)

                with open(temp_dir_path / "requirements.txt", "w") as f:
                    f.write(f"scikit-learn=={sklearn_version}\n")
                    f.write("skops\n")

                # Create model card
                self._create_hugging_face_model_card(str(temp_dir_path))

                # Create repo if it doesn't exist and upload
                api = HfApi(token=hf_hub_token)
                create_repo(repo_id, exist_ok=True, token=hf_hub_token)

                api.upload_folder(
                    folder_path=str(temp_dir_path),
                    repo_id=repo_id,
                    commit_message=commit_message,
                )

            except Exception as e:
                raise RuntimeError(f"Failed to push to Hugging Face Hub: {e}") from e
