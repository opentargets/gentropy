"""Step to build the Effector Gene List (EGL) used to seed the L2G gold standard.

The Effector Gene List is a clean, deduplicated set of ``(diseaseId, targetId)`` pairs
that represent trustworthy gene-disease relationships. It is assembled from up to three
independent sources produced by the Open Targets Platform ETL:

1. Rare-variant genetic evidence (e.g. ``eva``, ``genomics_england``, ``clingen`` ...),
   filtered on the evidence ``score``.
2. Clinical precedence evidence from ChEMBL, filtered on the drug ``clinicalStage``.
3. A previously curated gold standard (OTG), filtered on its confidence level.

Every source is optional so the list can be regenerated from whichever inputs are
available, and every source-specific filter is exposed as a parameter.
"""

from __future__ import annotations

import logging

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from gentropy.common.session import Session

logger = logging.getLogger(__name__)

# Clinical stages considered "approved enough" to seed a gene-disease pair.
DEFAULT_APPROVED_CLINICAL_PHASES: list[str] = [
    "PHASE_4",
    "APPROVAL",
    "PHASE_3",
    "PREAPPROVAL",
]

# Confidence levels retained from the legacy OTG gold standard curation.
DEFAULT_GOLD_STANDARD_CONFIDENCE: list[str] = ["High", "Medium"]


class EffectorGeneListStep:
    """Assemble the Effector Gene List from platform ETL evidence outputs."""

    def __init__(
        self,
        session: Session,
        *,
        effector_gene_list_path: str,
        rare_variant_evidence_paths: list[str] | None = None,
        rare_variant_score_threshold: float = 0.75,
        clinical_evidence_path: str | None = None,
        approved_clinical_phases: list[str] | None = None,
        gold_standard_path: str | None = None,
        gold_standard_confidence: list[str] | None = None,
    ) -> None:
        """Read the requested sources, combine them and write the EGL as parquet.

        Args:
            session (Session): Session object that contains the Spark session.
            effector_gene_list_path (str): Output path for the effector gene list parquet.
                The output contains the distinct ``diseaseId`` and ``targetId`` columns.
            rare_variant_evidence_paths (list[str] | None): Paths to the rare-variant
                evidence datasets (one per datasource, e.g. ``.../evidence_eva/``). They are
                unioned together allowing for missing columns. Defaults to None (source skipped).
            rare_variant_score_threshold (float): Minimum evidence ``score`` for a rare-variant
                pair to be retained. Defaults to 0.75.
            clinical_evidence_path (str | None): Path to the ChEMBL clinical precedence evidence
                dataset (e.g. ``.../evidence_clinical_precedence/``). Defaults to None (source skipped).
            approved_clinical_phases (list[str] | None): Clinical stages retained from the clinical
                evidence. Defaults to ``["PHASE_4", "APPROVAL", "PHASE_3", "PREAPPROVAL"]``.
            gold_standard_path (str | None): Path to the legacy OTG gold standard curation JSON.
                Defaults to None (source skipped).
            gold_standard_confidence (list[str] | None): Confidence levels retained from the gold
                standard curation. Defaults to ``["High", "Medium"]``.

        Raises:
            ValueError: If none of the three sources are provided.
        """
        approved_clinical_phases = (
            approved_clinical_phases
            if approved_clinical_phases is not None
            else list(DEFAULT_APPROVED_CLINICAL_PHASES)
        )
        gold_standard_confidence = (
            gold_standard_confidence
            if gold_standard_confidence is not None
            else list(DEFAULT_GOLD_STANDARD_CONFIDENCE)
        )

        sources: list[DataFrame] = []

        if rare_variant_evidence_paths:
            rare = self._rare_variant_pairs(
                session, rare_variant_evidence_paths, rare_variant_score_threshold
            )
            logger.info("Rare-variant source contributes %d pairs", rare.count())
            sources.append(rare)

        if clinical_evidence_path:
            clinical = self._clinical_pairs(
                session, clinical_evidence_path, approved_clinical_phases
            )
            logger.info("Clinical source contributes %d pairs", clinical.count())
            sources.append(clinical)

        if gold_standard_path:
            gold = self._gold_standard_pairs(
                session, gold_standard_path, gold_standard_confidence
            )
            logger.info("Gold standard source contributes %d pairs", gold.count())
            sources.append(gold)

        if not sources:
            raise ValueError(
                "At least one EGL source must be provided "
                "(rare_variant_evidence_paths, clinical_evidence_path or gold_standard_path)."
            )

        combined = sources[0]
        for extra in sources[1:]:
            combined = combined.unionByName(extra)
        combined = combined.dropDuplicates(["targetId", "diseaseId"])

        (
            combined.coalesce(session.output_partitions)
            .write.mode(session.write_mode)
            .parquet(effector_gene_list_path)
        )

    @staticmethod
    def _rare_variant_pairs(
        session: Session,
        evidence_paths: list[str],
        score_threshold: float,
    ) -> DataFrame:
        """Union rare-variant evidence datasets and extract high-scoring gene-disease pairs.

        Args:
            session (Session): Active session.
            evidence_paths (list[str]): One path per rare-variant datasource.
            score_threshold (float): Minimum evidence ``score`` to keep.

        Returns:
            DataFrame: Distinct ``diseaseId``, ``targetId`` pairs.
        """
        evidence: DataFrame | None = None
        for path in evidence_paths:
            df = session.load_data(path, "parquet")
            evidence = (
                df
                if evidence is None
                else evidence.unionByName(df, allowMissingColumns=True)
            )
        assert evidence is not None  # noqa: S101 - guaranteed by non-empty evidence_paths
        return (
            evidence.filter(f.col("score") >= score_threshold)
            .select("diseaseId", "targetId")
            .distinct()
        )

    @staticmethod
    def _clinical_pairs(
        session: Session,
        clinical_evidence_path: str,
        approved_clinical_phases: list[str],
    ) -> DataFrame:
        """Extract gene-disease pairs from clinical precedence evidence.

        Args:
            session (Session): Active session.
            clinical_evidence_path (str): Path to the clinical precedence evidence dataset.
            approved_clinical_phases (list[str]): Clinical stages to keep.

        Returns:
            DataFrame: Distinct ``diseaseId``, ``targetId`` pairs.
        """
        clinical = session.load_data(clinical_evidence_path, "parquet")
        return (
            clinical.filter(f.col("clinicalStage").isin(approved_clinical_phases))
            .select("diseaseId", "targetId")
            .distinct()
        )

    @staticmethod
    def _gold_standard_pairs(
        session: Session,
        gold_standard_path: str,
        gold_standard_confidence: list[str],
    ) -> DataFrame:
        """Extract gene-disease pairs from the legacy OTG gold standard curation.

        Args:
            session (Session): Active session.
            gold_standard_path (str): Path to the OTG gold standard curation JSON.
            gold_standard_confidence (list[str]): Confidence levels to keep.

        Returns:
            DataFrame: Distinct ``diseaseId``, ``targetId`` pairs. The ``diseaseId`` is one
                EFO term per row (the ontology array is exploded).
        """
        gold = session.load_data(gold_standard_path, "json")
        return (
            gold.filter(
                f.col("gold_standard_info.highest_confidence").isin(
                    gold_standard_confidence
                )
            )
            .select(
                f.col("gold_standard_info.gene_id").alias("targetId"),
                f.explode(f.col("trait_info.ontology")).alias("diseaseId"),
            )
            .select("diseaseId", "targetId")
            .distinct()
        )
