"""Annotate fine-mapping locus sets with ancestry-specific LD pairs."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, cast

from pyspark import StorageLevel

from gentropy.common.session import Session
from gentropy.dataset.fine_mapping_study_metadata import FineMappingStudyMetadata
from gentropy.dataset.multi_ancestry_pairwise_ld import MultiAncestryPairwiseLD
from gentropy.datasource.pan_ukbb_ld.ld import PanUKBBLDMatrix

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class FineMappingLocusSetLDAnnotationStep:
    """Extract LD pairs for all loci in one fine-mapping run."""

    def __init__(
        self,
        session: Session,
        fine_mapping_locus_set_input_path: str,
        fine_mapping_study_metadata_jsonl_input_path: str,
        multi_ancestry_pairwise_ld_output_path: str,
        stats_output_path: str,
        ld_registry: Sequence[Mapping[str, str]],
    ) -> None:
        """Run LD annotation for a fine-mapping locus-set dataset.

        Args:
            session (Session): Gentropy session.
            fine_mapping_locus_set_input_path (str): FineMappingLocusSet parquet path.
            fine_mapping_study_metadata_jsonl_input_path (str): Study metadata JSONL path.
            multi_ancestry_pairwise_ld_output_path (str): Output parquet path.
            stats_output_path (str): Output JSONL path for ancestry pair counts.
            ld_registry (Sequence[Mapping[str, str]]): LD references with
                ``ancestry``, ``vi_path``, and ``bm_path`` keys.
        """
        references = self._normalize_ld_registry(ld_registry)
        metadata = FineMappingStudyMetadata.from_jsonl(
            session, fine_mapping_study_metadata_jsonl_input_path
        )
        study_ancestries = {
            row["studyId"]: row["ancestry"]
            for row in metadata.df.select("studyId", "ancestry").collect()
        }
        self._validate_reference_coverage(list(study_ancestries.values()), references)
        locus_set = session.spark.read.parquet(fine_mapping_locus_set_input_path)
        self._validate_locus_set_schema(locus_set)
        locus_set = locus_set.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            locus_count = locus_set.count()
            session.logger.info(
                f"LD annotation checkpoint: loaded {locus_count} locus rows from "
                f"{fine_mapping_locus_set_input_path}"
            )
            index_by_ancestry = {
                ancestry: session.spark.read.parquet(reference["vi_path"])
                for ancestry, reference in references.items()
            }
            matrix_by_ancestry = {
                ancestry: PanUKBBLDMatrix(pan_ukbb_bm_path=reference["bm_path"])
                for ancestry, reference in references.items()
            }

            pairwise_datasets: list[DataFrame] = []
            requested_ancestries: list[str] = []
            for row in locus_set.select("studyId", "locus").toLocalIterator():
                study_id = row["studyId"]
                if study_id not in study_ancestries:
                    raise ValueError(f"No metadata found for studyId: {study_id}")
                ancestry = study_ancestries[study_id]
                requested_ancestries.append(ancestry)
                variants = self._locus_variant_ids(row["locus"])
                if not variants:
                    continue
                index = index_by_ancestry[ancestry]
                locus_index = index.filter(
                    index.variantId.isin(variants)
                ).dropDuplicates(["variantId"])
                locus_index = locus_index.persist(StorageLevel.MEMORY_AND_DISK)
                try:
                    matched_variant_count = locus_index.count()
                    session.logger.info(
                        f"LD annotation checkpoint: studyId={study_id} ancestry={ancestry} "
                        f"requested_variants={len(variants)} "
                        f"matched_index_variants={matched_variant_count}"
                    )
                    if matched_variant_count == 0:
                        continue
                    pairwise_datasets.append(
                        matrix_by_ancestry[ancestry].get_long_format_ld_matrix(
                            locus_index, ancestry
                        )
                    )
                finally:
                    locus_index.unpersist()

            output = self._union_pairwise_datasets(locus_set, pairwise_datasets)
            if self._needs_pair_deduplication(requested_ancestries):
                session.logger.info(
                    "LD annotation checkpoint: applying pair deduplication because ancestries are repeated"
                )
                output = output.dropDuplicates(
                    ["ancestry", "variantIdI", "variantIdJ"]
                )
            else:
                session.logger.info(
                    "LD annotation checkpoint: skipping pair deduplication because ancestries are unique"
                )
            output, output_count = self._persist_and_materialize(output)
            session.logger.info(
                f"LD annotation checkpoint: materialized output rows={output_count} "
                f"storage_level={output.storageLevel}"
            )
            try:
                output.coalesce(session.output_partitions).write.mode(
                    session.write_mode
                ).parquet(multi_ancestry_pairwise_ld_output_path)
                session.logger.info(
                    f"LD annotation checkpoint: wrote output path="
                    f"{multi_ancestry_pairwise_ld_output_path} "
                    f"partitions={session.output_partitions}"
                )
                stats = self._ld_pair_counts(
                    output, sorted(set(study_ancestries.values()))
                )
                self._write_ld_pair_stats(
                    stats,
                    stats_output_path,
                )
                session.logger.info(
                    f"LD annotation checkpoint: wrote stats path={stats_output_path} "
                    f"counts={stats}"
                )
            finally:
                output.unpersist()
        finally:
            locus_set.unpersist()

    @staticmethod
    def _persist_and_materialize(output: DataFrame) -> tuple[DataFrame, int]:
        """Persist and materialize output reused by writing and statistics actions.

        Args:
            output (DataFrame): Final LD-pair dataframe.

        Returns:
            tuple[DataFrame, int]: The persisted dataframe and its row count.
        """
        output = output.persist(StorageLevel.MEMORY_AND_DISK)
        return output, output.count()

    @staticmethod
    def _needs_pair_deduplication(requested_ancestries: Sequence[str]) -> bool:
        """Return whether multiple inputs can emit the same ancestry pair.

        Args:
            requested_ancestries (Sequence[str]): Ancestry for each input locus.

        Returns:
            bool: Whether the final union requires pair deduplication.
        """
        return len(requested_ancestries) != len(set(requested_ancestries))

    @staticmethod
    def _normalize_ld_registry(
        ld_registry: Sequence[Mapping[str, str]],
    ) -> dict[str, dict[str, str]]:
        """Validate and retain flat LD registry configuration.

        Args:
            ld_registry (Sequence[Mapping[str, str]]): Flat LD references.

        Returns:
            dict[str, dict[str, str]]: Registry records keyed by ancestry.
        """
        if not ld_registry:
            raise ValueError("At least one LD reference is required")
        normalized: dict[str, dict[str, str]] = {}
        for reference in ld_registry:
            missing = {"ancestry", "vi_path", "bm_path"} - reference.keys()
            if missing:
                raise ValueError(
                    f"LD reference is missing required keys: {sorted(missing)}"
                )
            ancestry = reference["ancestry"]
            if ancestry in normalized:
                raise ValueError(f"Duplicate LD reference ancestry: {ancestry}")
            normalized[ancestry] = {
                "ancestry": ancestry,
                "vi_path": reference["vi_path"],
                "bm_path": reference["bm_path"],
            }
        return normalized

    @staticmethod
    def _locus_variant_ids(locus: list[Mapping[str, object]] | None) -> list[str]:
        """Return unique variant IDs from one collected locus array.

        Args:
            locus (list[Mapping[str, object]] | None): Collected locus variants.

        Returns:
            list[str]: Unique variant IDs in input order.
        """
        if not locus:
            return []
        return list(
            dict.fromkeys(cast(str, variant["variantId"]) for variant in locus)
        )

    @staticmethod
    def _ld_pair_counts(
        pairwise_ld: DataFrame, requested_ancestries: Sequence[str]
    ) -> list[dict[str, int | str]]:
        """Count final LD rows for every requested ancestry.

        Args:
            pairwise_ld (DataFrame): Final deduplicated LD pair dataframe.
            requested_ancestries (Sequence[str]): Ancestries expected in this run.

        Returns:
            list[dict[str, int | str]]: Deterministically ordered ancestry counts, including zeroes.
        """
        observed = {
            row["ancestry"]: row["count"]
            for row in pairwise_ld.groupBy("ancestry").count().collect()
        }
        return [
            {"ancestry": ancestry, "n_ld_pairs": int(observed.get(ancestry, 0))}
            for ancestry in sorted(requested_ancestries)
        ]

    @staticmethod
    def _write_ld_pair_stats(
        stats: Sequence[Mapping[str, int | str]], path: str | Path
    ) -> None:
        """Write LD pair counts as compact JSONL records.

        Args:
            stats (Sequence[Mapping[str, int | str]]): Ancestry pair counts.
            path (str | Path): Destination JSONL path.
        """
        Path(path).write_text(
            "\n".join(json.dumps(record, separators=(",", ":")) for record in stats)
            + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def _validate_reference_coverage(
        study_ancestries: Sequence[str],
        references: Mapping[str, Mapping[str, str]],
    ) -> None:
        """Ensure every observed study ancestry has a configured LD reference.

        Args:
            study_ancestries (Sequence[str]): Ancestries observed in study metadata.
            references (Mapping[str, Mapping[str, str]]): Configured LD references.
        """
        missing = set(study_ancestries) - references.keys()
        if missing:
            raise ValueError(
                f"No LD reference configured for ancestries: {sorted(missing)}"
            )

    @staticmethod
    def _validate_locus_set_schema(locus_set: DataFrame) -> None:
        """Validate the minimum collector FineMappingLocusSet columns.

        Args:
            locus_set (DataFrame): Collector FineMappingLocusSet dataframe.
        """
        required = {"studyId", "locus"}
        missing = required - set(locus_set.columns)
        if missing:
            raise ValueError(
                f"FineMappingLocusSet is missing required columns: {sorted(missing)}"
            )

    @staticmethod
    def _union_pairwise_datasets(
        locus_set: DataFrame,
        pairwise_datasets: list[DataFrame],
    ) -> DataFrame:
        """Create a typed empty output or union all extracted pair tables.

        Args:
            locus_set (DataFrame): Source dataframe used to access its Spark session.
            pairwise_datasets (list[DataFrame]): Extracted LD pair dataframes.

        Returns:
            DataFrame: Unioned LD pairs with the multi-ancestry schema.
        """
        if pairwise_datasets:
            output = pairwise_datasets[0]
            for pairs in pairwise_datasets[1:]:
                output = output.unionByName(pairs)
            return output
        return locus_set.sparkSession.createDataFrame(
            [], MultiAncestryPairwiseLD.get_schema()
        )
