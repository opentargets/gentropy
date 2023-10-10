"""Dataset that contains the Locus to Gene predictions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Type

from pyspark.ml.functions import vector_to_array

from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import _convert_from_long_to_wide
from otg.dataset.dataset import Dataset
from otg.dataset.l2g.feature_matrix import L2GFeatureMatrix
from otg.method.locus_to_gene import LocusToGeneModel

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    from otg.common.session import Session


@dataclass
class L2GPredictions(Dataset):
    """Dataset that contains the Locus to Gene predictions.

    It is the result of applying the L2G model on the feature matrix, which contains all
    the study/locus pairs and their functional annotations. The score column informs the
    confidence of the prediction that a gene is causal to an association.
    """

    @classmethod
    def from_parquet(
        cls: Type[L2GPredictions], session: Session, path: str
    ) -> L2GPredictions:
        """Initialise L2G from parquet file.

        Args:
            session (Session): ETL session
            path (str): Path to parquet file

        Returns:
            L2GPredictions: Locus to gene predictions
        """
        return super().from_parquet(session, path, cls._schema)

    @classmethod
    def from_study_locus(
        cls: Type[L2GPredictions],
        session: Session,
        feature_matrix_path: str,
        model_path: str,
    ) -> L2GPredictions:
        """Initialise L2G from feature matrix.

        Args:
            session (Session): ETL session
            feature_matrix_path (str): Feature matrix
            model_path (str): Locus to gene model

        Returns:
            L2GPredictions: Locus to gene predictions
        """
        # fm = L2GFeatureMatrix.generate_features(
        #     session=self.session,
        #     study_locus_path=self.study_locus_path,
        #     study_index_path=self.study_index_path,
        #     variant_gene_path=self.variant_gene_path,
        #     colocalisation_path=self.colocalisation_path,
        # )

        fm = _convert_from_long_to_wide(
            session.spark.read.parquet(feature_matrix_path),
            id_vars=["studyLocusId", "geneId"],
            var_name="featureName",
            value_name="featureValue",
        ).transform(L2GFeatureMatrix.fill_na)

        return L2GPredictions(
            # Load and apply fitted model
            _df=LocusToGeneModel.load_from_disk(
                model_path, features_list=fm.drop("studyLocusId", "geneId").columns
            ).predict(fm)
            # the probability of the positive class is the second element inside the probability array
            # - this is selected as the L2G probability
            .select(
                "studyLocusId",
                "geneId",
                vector_to_array("probability")[1].alias("score"),
            )
        )

    @classmethod
    def get_schema(cls: type[L2GPredictions]) -> StructType:
        """Provides the schema for the L2GPredictions dataset."""
        return parse_spark_schema("l2g_predictions.json")
