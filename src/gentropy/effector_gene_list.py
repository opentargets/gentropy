"""Step to build the Effector Gene List (EGL) used to label the L2G training set.

The Effector Gene List is a deduplicated set of ``(diseaseId, targetId)`` pairs that
represent trustworthy gene-disease relationships. It is assembled from up to three
sources produced by the Open Targets Platform ETL:

1. Rare-variant genetic evidence (e.g. ``eva``, ``genomics_england``, ``clingen`` ...),
   filtered on the evidence ``score``.
2. Clinical precedence evidence from ChEMBL, filtered on the drug ``clinicalStage``.
3. The legacy OTG gold standard curation, filtered on its confidence level.

Every source is optional so the list can be regenerated from whichever inputs are
available, and every source-specific filter is exposed as a parameter.
"""

from __future__ import annotations

from collections.abc import Sequence
from functools import reduce

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from gentropy.common.session import Session

# Clinical stages considered "approved enough" to seed a gene-disease pair.
APPROVED_CLINICAL_STAGES = ("PHASE_4", "APPROVAL", "PHASE_3", "PREAPPROVAL")

# Confidence levels retained from the legacy OTG gold standard curation.
GOLD_STANDARD_CONFIDENCE = ("High", "Medium")


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
        approved_clinical_phases: Sequence[str] = APPROVED_CLINICAL_STAGES,
        gold_standard_path: str | None = None,
        gold_standard_confidence: Sequence[str] = GOLD_STANDARD_CONFIDENCE,
    ) -> None:
        """Read the requested sources, combine them and write the EGL as parquet.

        Args:
            session (Session): Session object that contains the Spark session.
            effector_gene_list_path (str): Output path for the effector gene list parquet,
                with the distinct ``diseaseId`` and ``targetId`` pairs.
            rare_variant_evidence_paths (list[str] | None): Paths to the rare-variant evidence
                datasets (one per datasource, e.g. ``.../evidence_eva/``). Defaults to None (source skipped).
            rare_variant_score_threshold (float): Minimum evidence ``score`` for a rare-variant
                pair to be retained. Defaults to 0.75.
            clinical_evidence_path (str | None): Path to the ChEMBL clinical precedence evidence
                dataset (e.g. ``.../evidence_clinical_precedence/``). Defaults to None (source skipped).
            approved_clinical_phases (Sequence[str]): Clinical stages retained from the clinical
                evidence. Defaults to ``APPROVED_CLINICAL_STAGES``.
            gold_standard_path (str | None): Path to the legacy OTG gold standard curation JSON.
                Defaults to None (source skipped).
            gold_standard_confidence (Sequence[str]): Confidence levels retained from the gold
                standard curation. Defaults to ``GOLD_STANDARD_CONFIDENCE``.

        Raises:
            ValueError: If none of the three sources are provided.
        """
        sources: list[DataFrame] = []

        if rare_variant_evidence_paths:
            session.logger.info("Including the rare-variant source")
            sources += [
                session.load_data(path, "parquet")
                .filter(f.col("score") >= rare_variant_score_threshold)
                .select("diseaseId", "targetId")
                for path in rare_variant_evidence_paths
            ]

        if clinical_evidence_path:
            session.logger.info("Including the clinical precedence source")
            sources.append(
                session.load_data(clinical_evidence_path, "parquet")
                .filter(f.col("clinicalStage").isin(list(approved_clinical_phases)))
                .select("diseaseId", "targetId")
            )

        if gold_standard_path:
            session.logger.info("Including the legacy gold standard source")
            sources.append(
                session.load_data(gold_standard_path, "json")
                .filter(
                    f.col("gold_standard_info.highest_confidence").isin(
                        list(gold_standard_confidence)
                    )
                )
                .select(
                    f.explode("trait_info.ontology").alias("diseaseId"),
                    f.col("gold_standard_info.gene_id").alias("targetId"),
                )
            )

        if not sources:
            raise ValueError(
                "At least one EGL source must be provided "
                "(rare_variant_evidence_paths, clinical_evidence_path or gold_standard_path)."
            )

        (
            reduce(DataFrame.unionByName, sources)
            .distinct()
            .coalesce(session.output_partitions)
            .write.mode(session.write_mode)
            .parquet(effector_gene_list_path)
        )
