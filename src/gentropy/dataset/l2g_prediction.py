"""Dataset that contains the Locus to Gene predictions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Type

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.session import Session
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.v2g import V2G
from gentropy.method.l2g.model import LocusToGeneModel

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
        credible_set: StudyLocus,
        study_index: StudyIndex,
        v2g: V2G,
        coloc: Colocalisation,
        session: Session,
    ) -> L2GPrediction:
        """Extract L2G predictions for a set of credible sets derived from GWAS.

        Args:
            model_path (str): Path to the fitted model
            features_list (list[str]): List of features to use for the model
            credible_set (StudyLocus): Credible set dataset
            study_index (StudyIndex): Study index dataset
            v2g (V2G): Variant to gene dataset
            coloc (Colocalisation): Colocalisation dataset
            session (Session): Session object that contains the Spark session

        Returns:
            L2GPrediction: L2G dataset
        """
        fm = L2GFeatureMatrix.generate_features(
            features_list=features_list,
            credible_set=credible_set,
            study_index=study_index,
            variant_gene=v2g,
            colocalisation=coloc,
        ).fill_na()

        gwas_fm = L2GFeatureMatrix(
            _df=(
                fm.df.join(
                    credible_set.filter_by_study_type("gwas", study_index).df,
                    on="studyLocusId",
                )
            ),
            _schema=cls.get_schema(),
        )
        l2g_model = LocusToGeneModel.load_from_disk(
            model_path,
            features_list=features_list,
        )
        return l2g_model.predict(gwas_fm, session)
