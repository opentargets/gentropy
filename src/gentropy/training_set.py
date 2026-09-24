"""Step to build the L2G training set from the Effector Gene List.

The training set labels the ``(studyLocusId, geneId)`` rows of the L2G feature matrix for
the credible sets holding at least one Effector Gene List (EGL) gene: a gene is a positive
when it is an EGL effector for a disease of the credible set's study, and every other gene
of those credible sets is a negative. Credible sets without an EGL gene are left out.

A series of optional, parametrised filters then clean the labels to reduce noise and
leakage:

* replication filter — keep only the credible sets carrying the ``REPLICATED`` quality
  control flag raised by ``StudyLocus.qc_replication``;
* maximum positives per locus — drop loci with more than ``max_gsp_per_locus`` positives;
* protein-protein interaction filter — drop negatives that interact (STRING) with a
  positive gene in the same locus;
* distance filter — drop positives that are the closest gene to the sentinel (footprint
  distance of zero) to avoid distance leakage;
* protein-coding filter — restrict the set to protein-coding genes;
* deduplication — collapse credible sets that share identical positive feature profiles.

The output is an ``L2GGoldStandard`` parquet, the input of ``LocusToGeneTrainTestSplitStep``.
"""

from __future__ import annotations

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window

from gentropy.common.session import Session
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus, StudyLocusQualityCheck

# Positive features that, with the sentinel variant, gene and diseases, define a credible
# set's profile for deduplication. The colocalisation features are rounded before comparison.
_DEDUP_FEATURES: list[str] = ["vepMaximum", "vepMean"]
_DEDUP_ROUNDED_FEATURES: list[str] = [
    "eQtlColocClppMaximum",
    "pQtlColocClppMaximum",
    "sQtlColocClppMaximum",
    "eQtlColocH4Maximum",
    "pQtlColocH4Maximum",
    "sQtlColocH4Maximum",
]


class TrainingSetStep:
    """Build the L2G training set from the Effector Gene List."""

    def __init__(
        self,
        session: Session,
        *,
        feature_matrix_path: str,
        credible_set_path: str,
        study_index_path: str,
        effector_gene_list_path: str,
        training_set_path: str,
        interaction_path: str | None = None,
        apply_replication_filter: bool = True,
        max_gsp_per_locus: int = 2,
        interaction_source: str = "string",
        interaction_score_threshold: float = 0.75,
        apply_distance_filter: bool = True,
        protein_coding_only: bool = True,
        apply_deduplication: bool = True,
    ) -> None:
        """Read the inputs, label and clean the feature matrix, and write the training set.

        Args:
            session (Session): Session object that contains the Spark session.
            feature_matrix_path (str): Path to the L2G feature matrix parquet.
            credible_set_path (str): Path to the validated credible set (StudyLocus) dataset,
                the output of ``StudyLocusValidationStep``, which raises the ``REPLICATED`` flag.
            study_index_path (str): Path to the study index dataset.
            effector_gene_list_path (str): Path to the Effector Gene List parquet produced by
                ``EffectorGeneListStep`` (columns ``diseaseId``, ``targetId``).
            training_set_path (str): Output path for the training set parquet.
            interaction_path (str | None): Path to the platform ``interaction`` dataset. When
                given, negatives interacting with a positive gene in the same locus are dropped.
                Defaults to None (filter skipped).
            apply_replication_filter (bool): Keep only the credible sets flagged as ``REPLICATED``
                by ``StudyLocus.qc_replication``. Defaults to True.
            max_gsp_per_locus (int): Maximum number of positives allowed per credible set; loci
                exceeding it are dropped. Defaults to 2.
            interaction_source (str): ``sourceDatabase`` value to keep from the interaction dataset.
                Defaults to "string".
            interaction_score_threshold (float): Minimum interaction ``scoring`` to keep. Defaults to 0.75.
            apply_distance_filter (bool): Drop positives with a sentinel footprint distance of zero
                (the closest gene) to avoid distance leakage. Defaults to True.
            protein_coding_only (bool): Restrict the training set to protein-coding genes. Defaults to True.
            apply_deduplication (bool): Collapse credible sets sharing identical positive feature
                profiles. Defaults to True.
        """
        credible_set = StudyLocus.from_parquet(session, credible_set_path).df
        if apply_replication_filter:
            credible_set = credible_set.filter(
                f.array_contains(
                    "qualityControls", StudyLocusQualityCheck.REPLICATED.value
                )
            )
        loci = credible_set.select("studyLocusId", "studyId", "variantId").join(
            StudyIndex.from_parquet(session, study_index_path).df.select(
                "studyId", "diseaseIds"
            ),
            on="studyId",
        )
        feature_matrix = session.load_data(feature_matrix_path, "parquet").join(
            loci, on="studyLocusId"
        )
        effector_gene_list = session.load_data(effector_gene_list_path, "parquet")

        labelled = self._label(feature_matrix, effector_gene_list)
        # Counted on the raw labels, before the filters below remove any positive.
        labelled = self._cap_positives_per_locus(labelled, max_gsp_per_locus)

        if interaction_path:
            labelled = self._drop_interacting_negatives(
                labelled,
                self._interaction_pairs(
                    session,
                    interaction_path,
                    interaction_source,
                    interaction_score_threshold,
                ),
            )
        if apply_distance_filter:
            labelled = labelled.filter(
                ~((f.col("GSP") == 1) & (f.col("distanceSentinelFootprint") == 0))
            )
        if protein_coding_only:
            labelled = labelled.filter(f.col("isProteinCoding") == 1)
        # The filters above can leave a locus without a positive.
        labelled = self._cap_positives_per_locus(labelled, max_gsp_per_locus)

        if apply_deduplication:
            labelled = self._deduplicate(labelled)

        L2GGoldStandard(
            _df=labelled.select(
                "studyLocusId",
                "variantId",
                "studyId",
                "geneId",
                f.when(f.col("GSP") == 1, L2GGoldStandard.GS_POSITIVE_LABEL)
                .otherwise(L2GGoldStandard.GS_NEGATIVE_LABEL)
                .alias("goldStandardSet"),
            ),
            _schema=L2GGoldStandard.get_schema(),
        ).df.coalesce(session.output_partitions).write.mode(session.write_mode).parquet(
            training_set_path
        )

    @staticmethod
    def _label(feature_matrix: DataFrame, effector_gene_list: DataFrame) -> DataFrame:
        """Keep the loci holding an EGL gene and flag the EGL genes with ``GSP``.

        A ``(studyLocusId, geneId)`` row is a positive when the gene is an EGL effector for any
        disease of the credible set's study.

        Args:
            feature_matrix (DataFrame): Feature matrix annotated with ``diseaseIds``.
            effector_gene_list (DataFrame): EGL with ``diseaseId`` and ``targetId`` columns.

        Returns:
            DataFrame: Rows of loci holding a positive, with ``GSP`` (1 positive, 0 negative).
        """
        positives = (
            feature_matrix.select(
                "studyLocusId", "geneId", f.explode("diseaseIds").alias("diseaseId")
            )
            .join(
                effector_gene_list.select(
                    "diseaseId", f.col("targetId").alias("geneId")
                ),
                on=["diseaseId", "geneId"],
                how="semi",
            )
            .select("studyLocusId", "geneId", f.lit(1).alias("GSP"))
            .distinct()
        )
        return (
            feature_matrix.join(positives, on="studyLocusId", how="semi")
            .join(positives, on=["studyLocusId", "geneId"], how="left")
            .fillna(0, subset=["GSP"])
        )

    @staticmethod
    def _cap_positives_per_locus(
        labelled: DataFrame, max_gsp_per_locus: int
    ) -> DataFrame:
        """Keep only loci with between one and ``max_gsp_per_locus`` positives.

        Args:
            labelled (DataFrame): Labelled feature matrix with a ``GSP`` column.
            max_gsp_per_locus (int): Maximum number of positives per credible set.

        Returns:
            DataFrame: Labelled rows restricted to the retained loci.
        """
        return (
            labelled.withColumn(
                "nPositives", f.sum("GSP").over(Window.partitionBy("studyLocusId"))
            )
            .filter(f.col("nPositives").between(1, max_gsp_per_locus))
            .drop("nPositives")
        )

    @staticmethod
    def _interaction_pairs(
        session: Session,
        interaction_path: str,
        interaction_source: str,
        interaction_score_threshold: float,
    ) -> DataFrame:
        """Load gene-gene interaction pairs above the score threshold.

        Pairs are used as stored: a negative is dropped when a positive of its locus is
        ``targetA`` and the negative is ``targetB``.

        Args:
            session (Session): Active session.
            interaction_path (str): Path to the interaction dataset.
            interaction_source (str): ``sourceDatabase`` to keep.
            interaction_score_threshold (float): Minimum interaction ``scoring``.

        Returns:
            DataFrame: ``targetA``, ``targetB`` interaction pairs.
        """
        return (
            session.load_data(interaction_path, "parquet", recursiveFileLookup=True)
            .filter(
                (f.col("sourceDatabase") == interaction_source)
                & (f.col("scoring") >= interaction_score_threshold)
            )
            .select("targetA", "targetB")
        )

    @staticmethod
    def _drop_interacting_negatives(
        labelled: DataFrame, interactions: DataFrame
    ) -> DataFrame:
        """Drop negative genes that interact with a positive gene in the same locus.

        Args:
            labelled (DataFrame): Labelled feature matrix with a ``GSP`` column.
            interactions (DataFrame): ``targetA``, ``targetB`` interaction pairs.

        Returns:
            DataFrame: Labelled rows with the interacting negatives removed.
        """
        negative_partners = (
            labelled.filter(f.col("GSP") == 1)
            .join(interactions, f.col("geneId") == f.col("targetA"))
            .select(
                "studyLocusId", f.col("targetB").alias("geneId"), f.lit(0).alias("GSP")
            )
        )
        return labelled.join(
            negative_partners, on=["studyLocusId", "geneId", "GSP"], how="anti"
        )

    @staticmethod
    def _deduplicate(labelled: DataFrame) -> DataFrame:
        """Keep one credible set per identical positive profile.

        A credible set's profile is its sorted disease set with the sorted list of its positives'
        gene, sentinel variant and features, with colocalisation rounded to 2 dp. Of the credible sets sharing a profile, the
        one with the smallest ``studyLocusId`` is kept.

        Args:
            labelled (DataFrame): Labelled feature matrix including ``variantId``.

        Returns:
            DataFrame: Labelled rows restricted to one credible set per profile.
        """
        positive_profile = f.struct(
            "geneId",
            "variantId",
            *_DEDUP_FEATURES,
            *[
                f.round(feature, 2).alias(feature)
                for feature in _DEDUP_ROUNDED_FEATURES
            ],
        )
        kept = (
            labelled.filter(f.col("GSP") == 1)
            .groupBy("studyLocusId")
            .agg(
                f.array_sort(f.first("diseaseIds")).alias("diseaseIds"),
                f.array_sort(f.collect_list(positive_profile)).alias("profile"),
            )
            .groupBy("diseaseIds", "profile")
            .agg(f.min("studyLocusId").alias("studyLocusId"))
        )
        return labelled.join(kept, on="studyLocusId", how="semi")
