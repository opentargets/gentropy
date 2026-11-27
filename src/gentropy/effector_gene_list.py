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
separate Plausible Effector Gene List (PEGL) instead of the EGL. The legacy gold standard
holds drug-derived records too (set ``ot_platform``) without a drug identifier; those on a
gene whose every drug is above the limit go to the PEGL as well. The PEGL is never used as
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

# Gold standard set of the drug-derived records, re-checked against max_targets_per_drug.
GOLD_STANDARD_DRUG_SET_LABEL = "ot_platform"


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
                the limit go to the PEGL, and so do gold standard ``ot_platform`` records on a gene
                whose every drug is above the limit; any pair already in the EGL is then removed
                from the PEGL. Defaults to None (no limit, no PEGL).
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
        plausible: list[DataFrame] = []

        if rare_variant_evidence_paths:
            session.logger.info("Including the rare-variant source")
            sources += [
                session.load_data(path, "parquet")
                .filter(f.col("score") >= rare_variant_score_threshold)
                .select("diseaseId", "targetId")
                for path in rare_variant_evidence_paths
            ]

        multi_target_only_targets: DataFrame | None = None
        if clinical_evidence_path:
            session.logger.info("Including the clinical precedence source")
            clinical = session.load_data(clinical_evidence_path, "parquet")
            approved = clinical.filter(
                f.col("clinicalStage").isin(list(approved_clinical_phases))
            )
            if max_targets_per_drug is None:
                sources.append(approved.select("diseaseId", "targetId"))
            else:
                multi_target_drugs, multi_target_only_targets = (
                    self._multi_target_drugs(clinical, max_targets_per_drug)
                )
                session.logger.info(
                    f"Drugs with more than {max_targets_per_drug} targets: "
                    f"{multi_target_drugs.count()}"
                )
                sources.append(
                    approved.join(multi_target_drugs, on="drugId", how="left_anti")
                    .select("diseaseId", "targetId")
                    .distinct()
                )
                plausible.append(
                    approved.join(multi_target_drugs, on="drugId", how="left_semi")
                    .select("diseaseId", "targetId")
                    .distinct()
                )

        if gold_standard_path:
            session.logger.info("Including the legacy gold standard source")
            gold_standard = session.load_data(gold_standard_path, "json").filter(
                f.col("gold_standard_info.highest_confidence").isin(
                    list(gold_standard_confidence)
                )
            )
            pairs = [
                f.explode("trait_info.ontology").alias("diseaseId"),
                f.col("gold_standard_info.gene_id").alias("targetId"),
            ]
            if multi_target_only_targets is None:
                sources.append(gold_standard.select(*pairs))
            else:
                # Drug-derived records carry no drugId, so they are matched on the gene: a record
                # is plausible when every drug of its gene is above the limit.
                gold_standard = gold_standard.select(
                    *pairs, f.col("metadata.set_label").alias("setLabel")
                ).join(
                    multi_target_only_targets.withColumn(
                        "isMultiTargetOnly", f.lit(True)
                    ),
                    on="targetId",
                    how="left",
                )
                is_plausible = (
                    f.col("setLabel") == GOLD_STANDARD_DRUG_SET_LABEL
                ) & f.col("isMultiTargetOnly").isNotNull()
                sources.append(
                    gold_standard.filter(~f.coalesce(is_plausible, f.lit(False)))
                    .select("diseaseId", "targetId")
                    .distinct()
                )
                plausible.append(
                    gold_standard.filter(is_plausible)
                    .select("diseaseId", "targetId")
                    .distinct()
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
            # Without a clinical source there are no multi-target drugs: the PEGL is written
            # empty so downstream steps can rely on the path.
            plausible_pairs = reduce(
                DataFrame.unionByName,
                plausible,
                session.spark.createDataFrame([], EffectorGeneList.get_schema()),
            ).distinct()
            # A pair with any other EGL support stays a positive: the two lists never overlap.
            plausible_effector_gene_list = EffectorGeneList(
                _df=plausible_pairs.join(
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
                f"PEGL pairs: {plausible_pairs.count()} before and "
                f"{plausible_effector_gene_list.df.count()} after removing EGL pairs"
            )
            plausible_effector_gene_list.unpersist()
        effector_gene_list.unpersist()

    @staticmethod
    def _multi_target_drugs(
        clinical: DataFrame, max_targets_per_drug: int
    ) -> tuple[DataFrame, DataFrame]:
        """Find the drugs above the target limit and the targets reached only by such drugs.

        Targets are counted per ``drugId`` over every clinical evidence row (all stages and
        diseases), before the stage filter.

        Args:
            clinical (DataFrame): Clinical precedence evidence with ``drugId`` and ``targetId``.
            max_targets_per_drug (int): Maximum number of distinct targets per drug.

        Returns:
            tuple[DataFrame, DataFrame]: The ``drugId`` of drugs with more than
                ``max_targets_per_drug`` targets, and the ``targetId`` of targets whose every drug
                has more than ``max_targets_per_drug`` targets.
        """
        drug_targets = clinical.select("drugId", "targetId").distinct()
        targets_per_drug = drug_targets.groupBy("drugId").agg(
            f.count("targetId").alias("nTargets")
        )
        multi_target_drugs = targets_per_drug.filter(
            f.col("nTargets") > max_targets_per_drug
        ).select("drugId")
        multi_target_only_targets = (
            drug_targets.join(targets_per_drug, on="drugId")
            .groupBy("targetId")
            .agg(f.min("nTargets").alias("minTargetsPerDrug"))
            .filter(f.col("minTargetsPerDrug") > max_targets_per_drug)
            .select("targetId")
        )
        return multi_target_drugs, multi_target_only_targets
