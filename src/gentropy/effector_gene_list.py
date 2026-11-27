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

Some ChEMBL drugs are mapped to every subunit or family member of their target (e.g.
metformin to the mitochondrial complex I genes). When ``max_targets_per_drug`` is set,
clinical pairs that come only from drugs with more targets than the limit are written to a
separate Plausible Effector Gene List (PEGL) instead of the EGL. The PEGL is never used as
positives and can be passed to ``TrainingSetStep`` to keep its pairs out of the negatives.
"""

from __future__ import annotations

from collections.abc import Sequence
from functools import reduce

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from gentropy.common.session import Session
from gentropy.dataset.effector_gene_list import EffectorGeneList

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
        max_targets_per_drug: int | None = None,
        plausible_effector_gene_list_path: str | None = None,
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
            max_targets_per_drug (int | None): Maximum number of distinct targets a drug may have
                for its clinical pairs to enter the EGL. Targets are counted per ``drugId`` over all
                clinical evidence rows, before the stage filter. Approved-stage pairs of drugs above
                the limit go to the PEGL, minus any pair already in the EGL. Defaults to None (no
                limit, no PEGL).
            plausible_effector_gene_list_path (str | None): Output path for the Plausible Effector
                Gene List parquet. Required when ``max_targets_per_drug`` is set. Defaults to None.

        Raises:
            ValueError: If none of the three sources are provided, or if ``max_targets_per_drug``
                is set without ``plausible_effector_gene_list_path``.
        """
        if max_targets_per_drug is not None and not plausible_effector_gene_list_path:
            raise ValueError(
                "plausible_effector_gene_list_path is required when max_targets_per_drug is set."
            )

        sources: list[DataFrame] = []
        plausible: DataFrame | None = None

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
            clinical = session.load_data(clinical_evidence_path, "parquet")
            if max_targets_per_drug is None:
                sources.append(
                    clinical.filter(
                        f.col("clinicalStage").isin(list(approved_clinical_phases))
                    ).select("diseaseId", "targetId")
                )
            else:
                clinical, plausible = self._split_multi_target_drugs(
                    session, clinical, approved_clinical_phases, max_targets_per_drug
                )
                sources.append(clinical)

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

        effector_gene_list = EffectorGeneList(
            _df=reduce(DataFrame.unionByName, sources).distinct(),
            _schema=EffectorGeneList.get_schema(),
        ).persist()
        effector_gene_list.df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(effector_gene_list_path)
        session.logger.info(f"EGL pairs: {effector_gene_list.df.count()}")

        if max_targets_per_drug is not None and plausible_effector_gene_list_path:
            # Without a clinical source there are no multi-target drugs: write an empty PEGL so
            # downstream steps can rely on the path.
            if plausible is None:
                plausible = session.spark.createDataFrame(
                    [], EffectorGeneList.get_schema()
                )
            # A pair with any other EGL support stays a positive: the two lists never overlap.
            plausible_effector_gene_list = EffectorGeneList(
                _df=plausible.join(
                    effector_gene_list.df,
                    on=["diseaseId", "targetId"],
                    how="left_anti",
                ),
                _schema=EffectorGeneList.get_schema(),
            ).persist()
            plausible_effector_gene_list.df.coalesce(
                session.output_partitions
            ).write.mode(session.write_mode).parquet(plausible_effector_gene_list_path)
            session.logger.info(
                f"PEGL pairs: {plausible.count()} before and "
                f"{plausible_effector_gene_list.df.count()} after removing EGL pairs"
            )
            plausible_effector_gene_list.unpersist()
        effector_gene_list.unpersist()

    @staticmethod
    def _split_multi_target_drugs(
        session: Session,
        clinical: DataFrame,
        approved_clinical_phases: Sequence[str],
        max_targets_per_drug: int,
    ) -> tuple[DataFrame, DataFrame]:
        """Split approved-stage clinical pairs by the number of targets of their drug.

        Targets are counted per ``drugId`` over every clinical evidence row (all stages and
        diseases), before the stage filter.

        Args:
            session (Session): Session object, used for logging.
            clinical (DataFrame): Clinical precedence evidence with ``drugId``, ``targetId``,
                ``diseaseId`` and ``clinicalStage``.
            approved_clinical_phases (Sequence[str]): Clinical stages retained.
            max_targets_per_drug (int): Maximum number of distinct targets per drug.

        Returns:
            tuple[DataFrame, DataFrame]: Distinct ``(diseaseId, targetId)`` pairs from drugs within
                the limit, and those from drugs above it.
        """
        targets_per_drug = clinical.groupBy("drugId").agg(
            f.countDistinct("targetId").alias("nTargets")
        )
        multi_target_drugs = targets_per_drug.filter(
            f.col("nTargets") > max_targets_per_drug
        ).select("drugId")
        session.logger.info(
            f"Drugs with more than {max_targets_per_drug} targets: "
            f"{multi_target_drugs.count()}"
        )
        approved = clinical.filter(
            f.col("clinicalStage").isin(list(approved_clinical_phases))
        )
        return (
            approved.join(multi_target_drugs, on="drugId", how="left_anti")
            .select("diseaseId", "targetId")
            .distinct(),
            approved.join(multi_target_drugs, on="drugId", how="left_semi")
            .select("diseaseId", "targetId")
            .distinct(),
        )
