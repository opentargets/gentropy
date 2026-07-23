"""Step to build the L2G training set (gold standard) from the Effector Gene List.

The training set labels every ``(studyLocusId, geneId)`` row of the L2G feature matrix
as ``positive`` or ``negative`` (``goldStandardSet``). Positives are the gene-disease
pairs of the Effector Gene List (EGL) that map onto a credible set for the matching
disease; negatives are the remaining genes in those loci.

A series of optional, parametrised filters clean the raw labelling to reduce noise and
leakage before the set is used to train the L2G model:

* replication filter — keep only credible sets whose variant-disease pair replicates
  across at least ``min_replication_studies`` GWAS studies;
* maximum positives per locus — drop loci with more than ``max_gsp_per_locus`` positives;
* protein-protein interaction filter — drop negatives that interact (STRING) with a
  positive gene in the same locus;
* distance filter — drop positives that are the closest gene to the sentinel (footprint
  distance of zero) to avoid distance leakage;
* protein-coding filter — restrict the set to protein-coding genes;
* deduplication — collapse credible sets that share identical positive feature profiles.

The output is written as JSON with the columns ``studyLocusId``, ``geneId``,
``diseaseIds``, ``variantId``, ``studyId`` and ``goldStandardSet``.
"""

from __future__ import annotations

import logging

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus

logger = logging.getLogger(__name__)

# Feature-matrix columns used, together with the rounded colocalisation columns, as the
# key for deduplicating credible sets with identical positive feature profiles.
_DEDUP_KEY_COLUMNS: list[str] = [
    "geneId",
    "diseaseIds",
    "variantId",
    "vepMaximum",
    "vepMean",
]
_DEDUP_ROUNDED_COLUMNS: list[str] = [
    "eQtlColocClppMaximum",
    "pQtlColocClppMaximum",
    "sQtlColocClppMaximum",
    "eQtlColocH4Maximum",
    "pQtlColocH4Maximum",
    "sQtlColocH4Maximum",
]


class TrainingSetStep:
    """Build the L2G gold standard training set from the Effector Gene List."""

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
        min_replication_studies: int = 2,
        max_gsp_per_locus: int = 2,
        apply_interaction_filter: bool = True,
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
            credible_set_path (str): Path to the credible set (StudyLocus) dataset.
            study_index_path (str): Path to the study index dataset.
            effector_gene_list_path (str): Path to the Effector Gene List parquet produced by
                ``EffectorGeneListStep`` (columns ``diseaseId``, ``targetId``).
            training_set_path (str): Output path for the training set JSON.
            interaction_path (str | None): Path to the gene interaction dataset (e.g. the platform
                ``interaction`` output). Required when ``apply_interaction_filter`` is True.
            apply_replication_filter (bool): Keep only credible sets whose variant-disease pair is
                seen in at least ``min_replication_studies`` GWAS studies. Defaults to True.
            min_replication_studies (int): Replication threshold. Defaults to 2.
            max_gsp_per_locus (int): Maximum number of positives allowed per credible set; loci
                exceeding it are dropped. Defaults to 2.
            apply_interaction_filter (bool): Drop negatives that interact with a positive gene in
                the same locus. Defaults to True.
            interaction_source (str): ``sourceDatabase`` value to keep from the interaction dataset.
                Defaults to "string".
            interaction_score_threshold (float): Minimum interaction ``scoring`` to keep. Defaults to 0.75.
            apply_distance_filter (bool): Drop positives with a sentinel footprint distance of zero
                (the closest gene) to avoid distance leakage. Defaults to True.
            protein_coding_only (bool): Restrict the training set to protein-coding genes. Defaults to True.
            apply_deduplication (bool): Collapse credible sets sharing identical positive feature
                profiles. Defaults to True.

        Raises:
            ValueError: If the interaction filter is requested without an ``interaction_path``.
        """
        if apply_interaction_filter and not interaction_path:
            raise ValueError(
                "interaction_path is required when apply_interaction_filter is True."
            )

        feature_matrix = session.load_data(feature_matrix_path, "parquet")
        credible_set = StudyLocus.from_parquet(
            session, credible_set_path, recursiveFileLookup=True
        )
        study_index = StudyIndex.from_parquet(
            session, study_index_path, recursiveFileLookup=True
        )
        effector_gene_list = session.load_data(effector_gene_list_path, "parquet")

        # 1. Attach study and disease context, then label positives against the EGL.
        annotated_fm = self._annotate_feature_matrix(
            feature_matrix, credible_set, study_index
        )
        labelled = self._label_gold_standard(annotated_fm, effector_gene_list)

        # 2. Optional replication filter.
        if apply_replication_filter:
            replicated_loci = self._replicated_loci(
                credible_set, study_index, min_replication_studies
            )
            labelled = labelled.join(replicated_loci, on="studyLocusId", how="inner")

        # 3. Cap the number of positives per locus.
        labelled = self._cap_positives_per_locus(labelled, max_gsp_per_locus)

        # 4. Optional protein-protein interaction filter.
        if apply_interaction_filter:
            assert interaction_path is not None  # noqa: S101 - guaranteed by the guard above
            interactions = self._interaction_pairs(
                session,
                interaction_path,
                interaction_source,
                interaction_score_threshold,
            )
            labelled = self._filter_interacting_negatives(labelled, interactions)

        # 5. Optional distance-leakage filter.
        if apply_distance_filter:
            labelled = labelled.filter(
                ~((f.col("GSP") == 1) & (f.col("distanceSentinelFootprint") == 0))
            )

        # 6. Optional protein-coding restriction (re-caps positives per locus afterwards).
        if protein_coding_only:
            labelled = labelled.filter(f.col("isProteinCoding") == 1)
            labelled = self._cap_positives_per_locus(labelled, max_gsp_per_locus)

        # 7. Attach the sentinel variant id (needed for dedup and output).
        labelled = labelled.join(
            credible_set.df.select("studyLocusId", "variantId"),
            on="studyLocusId",
            how="left",
        )

        # 8. Optional deduplication of credible sets with identical positive profiles.
        if apply_deduplication:
            labelled = self._deduplicate(labelled)

        self._write_training_set(session, labelled, training_set_path)

    @staticmethod
    def _annotate_feature_matrix(
        feature_matrix: DataFrame,
        credible_set: StudyLocus,
        study_index: StudyIndex,
    ) -> DataFrame:
        """Attach ``studyId`` and ``diseaseIds`` to every feature-matrix row.

        Args:
            feature_matrix (DataFrame): Raw L2G feature matrix.
            credible_set (StudyLocus): Credible set dataset providing ``studyId``.
            study_index (StudyIndex): Study index providing ``diseaseIds``.

        Returns:
            DataFrame: Feature matrix annotated with ``studyId`` and ``diseaseIds``.
        """
        cs = credible_set.df.select("studyLocusId", "studyId")
        studies = study_index.df.select("studyId", "diseaseIds").dropDuplicates(
            ["studyId"]
        )
        return feature_matrix.join(cs, on="studyLocusId", how="left").join(
            studies, on="studyId", how="left"
        )

    @staticmethod
    def _label_gold_standard(
        annotated_fm: DataFrame,
        effector_gene_list: DataFrame,
    ) -> DataFrame:
        """Label the loci that contain an EGL positive, flagging positive rows with ``GSP``.

        A ``(studyLocusId, geneId)`` row is a positive when the gene is an EGL effector for a
        disease assigned to the credible set. Only loci that contain at least one positive are
        retained; every other gene in those loci becomes a negative.

        Args:
            annotated_fm (DataFrame): Feature matrix annotated with ``studyId`` and ``diseaseIds``.
            effector_gene_list (DataFrame): EGL with ``diseaseId`` and ``targetId`` columns.

        Returns:
            DataFrame: Rows of loci containing a positive, with a ``GSP`` column (1 positive, 0 negative).
        """
        egl = effector_gene_list.select("targetId", "diseaseId").withColumnRenamed(
            "targetId", "geneId_egl"
        )
        positives = (
            annotated_fm.join(
                egl,
                (f.array_contains(annotated_fm["diseaseIds"], egl["diseaseId"]))
                & (annotated_fm["geneId"] == egl["geneId_egl"]),
                how="inner",
            )
            .select("studyLocusId", "geneId")
            .distinct()
        )

        loci_with_positive = positives.select("studyLocusId").distinct()
        return (
            annotated_fm.join(loci_with_positive, on="studyLocusId", how="inner")
            .join(
                positives.withColumn("GSP", f.lit(1)),
                on=["studyLocusId", "geneId"],
                how="left",
            )
            .withColumn(
                "GSP", f.when(f.col("GSP").isNotNull(), 1).otherwise(0)
            )
        )

    @staticmethod
    def _replicated_loci(
        credible_set: StudyLocus,
        study_index: StudyIndex,
        min_replication_studies: int,
    ) -> DataFrame:
        """Find credible sets whose variant-disease pair replicates across GWAS studies.

        A variant-disease pair replicates when at least ``min_replication_studies`` distinct
        study contexts (cohorts, publication and LD structure) report it.

        Args:
            credible_set (StudyLocus): Credible set dataset.
            study_index (StudyIndex): Study index dataset.
            min_replication_studies (int): Minimum number of distinct study contexts.

        Returns:
            DataFrame: Distinct ``studyLocusId`` values that pass the replication threshold.
        """
        studies = study_index.df.select(
            "studyId",
            "diseaseIds",
            "cohorts",
            "pubmedId",
            "ldPopulationStructure",
        )
        gwas = (
            credible_set.df.select(
                "studyId", "variantId", "studyType", "studyLocusId"
            )
            .join(studies, on="studyId", how="left")
            .filter(f.col("studyType") == "gwas")
            .withColumn("diseaseId", f.explode(f.col("diseaseIds")))
        )
        replicated_pairs = (
            gwas.select(
                "variantId", "diseaseId", "cohorts", "pubmedId", "ldPopulationStructure"
            )
            .dropDuplicates()
            .groupBy("variantId", "diseaseId")
            .agg(f.count("*").alias("count"))
            .filter(f.col("count") >= min_replication_studies)
        )
        return (
            gwas.join(replicated_pairs, on=["variantId", "diseaseId"], how="inner")
            .select("studyLocusId")
            .distinct()
        )

    @staticmethod
    def _cap_positives_per_locus(labelled: DataFrame, max_gsp_per_locus: int) -> DataFrame:
        """Keep only loci with between one and ``max_gsp_per_locus`` positives.

        Args:
            labelled (DataFrame): Labelled feature matrix with a ``GSP`` column.
            max_gsp_per_locus (int): Maximum number of positives per credible set.

        Returns:
            DataFrame: Labelled rows restricted to the retained loci.
        """
        counts = (
            labelled.filter(f.col("GSP") == 1)
            .groupBy("studyLocusId")
            .agg(f.count("*").alias("count"))
        )
        keep = counts.filter(
            (f.col("count") > 0) & (f.col("count") <= max_gsp_per_locus)
        ).select("studyLocusId")
        return labelled.join(keep, on="studyLocusId", how="inner")

    @staticmethod
    def _interaction_pairs(
        session: Session,
        interaction_path: str,
        interaction_source: str,
        interaction_score_threshold: float,
    ) -> DataFrame:
        """Load directed gene-gene interaction pairs above the score threshold.

        Args:
            session (Session): Active session.
            interaction_path (str): Path to the interaction dataset.
            interaction_source (str): ``sourceDatabase`` to keep.
            interaction_score_threshold (float): Minimum interaction ``scoring``.

        Returns:
            DataFrame: Distinct ``targetA``, ``targetB`` interaction pairs (self-interactions removed).
        """
        interactions = session.load_data(
            interaction_path, "parquet", recursiveFileLookup=True
        )
        return (
            interactions.filter(
                (f.col("sourceDatabase") == interaction_source)
                & (f.col("scoring") >= interaction_score_threshold)
                & (f.col("targetA") != f.col("targetB"))
            )
            .select("targetA", "targetB")
            .distinct()
        )

    @staticmethod
    def _filter_interacting_negatives(
        labelled: DataFrame, interactions: DataFrame
    ) -> DataFrame:
        """Drop negative genes that interact with a positive gene in the same locus.

        For every locus, the interaction partners of its positive genes are compared with its
        negatives. A negative that coincides with a partner in the same locus is removed, so a
        gene physically coupled to the true effector is not learned as a negative example.

        Args:
            labelled (DataFrame): Labelled feature matrix with a ``GSP`` column.
            interactions (DataFrame): Directed ``targetA``, ``targetB`` interaction pairs.

        Returns:
            DataFrame: Labelled rows with the interacting negatives removed.
        """
        positive_partners = (
            labelled.filter(f.col("GSP") == 1)
            .select("geneId", "studyLocusId")
            .join(interactions, f.col("geneId") == interactions["targetA"], how="inner")
            .select(f.col("targetB").alias("geneId"), "studyLocusId")
        )
        negatives = labelled.filter(f.col("GSP") == 0).select("geneId", "studyLocusId")
        # A negative gene that also appears as a positive's interaction partner in the same
        # locus shows up (at least) twice in the union: flag those for removal.
        to_remove = (
            negatives.union(positive_partners)
            .groupBy("geneId", "studyLocusId")
            .agg(f.count("*").alias("count"))
            .filter(f.col("count") >= 2)
            .select("geneId", "studyLocusId")
        )
        return labelled.join(to_remove, on=["geneId", "studyLocusId"], how="anti")

    @staticmethod
    def _deduplicate(labelled: DataFrame) -> DataFrame:
        """Collapse credible sets whose positives share an identical feature profile.

        Colocalisation feature values are rounded before comparison so numerically-equivalent
        loci are treated as duplicates. Only the credible sets whose positives survive the
        deduplication are kept.

        Args:
            labelled (DataFrame): Labelled feature matrix including ``variantId``.

        Returns:
            DataFrame: Labelled rows restricted to the deduplicated credible sets.
        """
        positives = labelled.filter(f.col("GSP") == 1)
        for col in _DEDUP_ROUNDED_COLUMNS:
            positives = positives.withColumn(col, f.round(f.col(col), 2))
        deduped = positives.dropDuplicates(_DEDUP_KEY_COLUMNS + _DEDUP_ROUNDED_COLUMNS)
        loci_to_keep = deduped.select("studyLocusId").distinct()
        return labelled.join(loci_to_keep, on="studyLocusId", how="inner")

    @staticmethod
    def _write_training_set(
        session: Session, labelled: DataFrame, training_set_path: str
    ) -> None:
        """Map ``GSP`` to ``goldStandardSet`` and write the training set as JSON.

        Args:
            session (Session): Active session.
            labelled (DataFrame): Cleaned, labelled feature matrix with a ``GSP`` column.
            training_set_path (str): Output JSON path.
        """
        training_set = labelled.select(
            "studyLocusId",
            "geneId",
            "diseaseIds",
            "variantId",
            "studyId",
            f.when(f.col("GSP") == 1, f.lit("positive"))
            .otherwise(f.lit("negative"))
            .alias("goldStandardSet"),
        )
        (
            training_set.coalesce(session.output_partitions)
            .write.mode(session.write_mode)
            .json(training_set_path)
        )
