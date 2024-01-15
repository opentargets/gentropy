"""Dataset that contains the Locus to Gene predictions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Type

import pyspark.sql.functions as f
from pyspark.ml.functions import vector_to_array

from oxygen.common.schemas import parse_spark_schema
from oxygen.dataset.colocalisation import Colocalisation
from oxygen.dataset.dataset import Dataset
from oxygen.dataset.l2g_feature_matrix import L2GFeatureMatrix
from oxygen.dataset.study_index import StudyIndex
from oxygen.dataset.study_locus import StudyLocus
from oxygen.dataset.v2g import V2G
from oxygen.method.l2g.model import LocusToGeneModel

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class L2GPrediction(Dataset):
    """Dataset that contains the Locus to Gene predictions.

    It is the result of applying the L2G model on a feature matrix, which contains all
    the study/locus pairs and their functional annotations. The score column informs the
    confidence of the prediction that a gene is causal to an association.
    """

    @classmethod
    def get_schema(cls: type[L2GPrediction]) -> StructType:
        """Provides the schema for the L2GPrediction dataset.

        Returns:
            StructType: Schema for the L2GPrediction dataset
        """
        return parse_spark_schema("l2g_predictions.json")

    @classmethod
    def from_credible_set(
        cls: Type[L2GPrediction],
        model_path: str,
        features_list: list[str],
        study_locus: StudyLocus,
        study_index: StudyIndex,
        v2g: V2G,
        coloc: Colocalisation,
    ) -> L2GPrediction:
        """Extract L2G predictions for a set of credible sets derived from GWAS.

        Args:
            model_path (str): Path to the fitted model
            features_list (list[str]): List of features to use for the model
            study_locus (StudyLocus): Study locus dataset
            study_index (StudyIndex): Study index dataset
            v2g (V2G): Variant to gene dataset
            coloc (Colocalisation): Colocalisation dataset

        Returns:
            L2GPrediction: L2G dataset
        """
        gwas_study_locus = StudyLocus(
            _df=study_locus.df.join(
                study_index.study_type_lut().filter(f.col("studyType") == "gwas"),
                on="studyId",
                how="inner",
            ),
            _schema=StudyLocus.get_schema(),
        )
        fm = L2GFeatureMatrix.generate_features(
            features_list=features_list,
            study_locus=gwas_study_locus,
            study_index=study_index,
            variant_gene=v2g,
            colocalisation=coloc,
        ).fill_na()
        return L2GPrediction(
            # Load and apply fitted model
            _df=(
                LocusToGeneModel.load_from_disk(
                    model_path,
                    features_list=features_list,
                )
                .predict(fm)
                # the probability of the positive class is the second element inside the probability array
                # - this is selected as the L2G probability
                .select(
                    "studyLocusId",
                    "geneId",
                    vector_to_array(f.col("probability"))[1].alias("score"),
                )
            ),
            _schema=cls.get_schema(),
        )
