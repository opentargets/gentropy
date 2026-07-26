"""Annotate fine-mapping locus sets with ancestry-specific LD pairs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from gentropy.common.session import Session
from gentropy.dataset.fine_mapping_study_metadata import FineMappingStudyMetadata
from gentropy.dataset.multi_ancestry_pairwise_ld import MultiAncestryPairwiseLD
from gentropy.datasource.pan_ukbb_ld.ld import (
    PanUKBBLDMatrix,
    normalize_pan_ukbb_population,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class FineMappingLocusSetLDAnnotationStep:
    """Extract LD pairs for all loci in one fine-mapping run."""

    def __init__(
        self,
        session: Session,
        fine_mapping_locus_set_input_path: str,
        fine_mapping_study_metadata_input_path: str,
        multi_ancestry_pairwise_ld_output_path: str,
        ld_references: Sequence[Mapping[str, str]],
    ) -> None:
        """Run LD annotation for a fine-mapping locus-set dataset.

        Args:
            session (Session): Gentropy session.
            fine_mapping_locus_set_input_path (str): FineMappingLocusSet parquet path.
            fine_mapping_study_metadata_input_path (str): Study metadata parquet path.
            multi_ancestry_pairwise_ld_output_path (str): Output parquet path.
            ld_references (Sequence[Mapping[str, str]]): LD references with
                ``ancestry``, ``vi_path``, and ``bm_path`` keys.
        """
        references = self._normalize_ld_references(ld_references)
        metadata = FineMappingStudyMetadata.from_parquet(
            session, fine_mapping_study_metadata_input_path
        )
        study_ancestries = {
            row["studyId"]: normalize_pan_ukbb_population(row["ancestry"])
            for row in metadata.df.select("studyId", "ancestry").collect()
        }
        self._validate_reference_coverage(list(study_ancestries.values()), references)
        locus_set = session.spark.read.parquet(fine_mapping_locus_set_input_path)
        self._validate_locus_set_schema(locus_set)
        index_by_ancestry = {
            ancestry: session.spark.read.parquet(reference["vi_path"])
            for ancestry, reference in references.items()
        }
        matrix_by_ancestry = {
            ancestry: PanUKBBLDMatrix(pan_ukbb_bm_path=reference["bm_path"])
            for ancestry, reference in references.items()
        }

        pairwise_datasets: list[DataFrame] = []
        for row in locus_set.select("studyId", "locus").toLocalIterator():
            study_id = row["studyId"]
            if study_id not in study_ancestries:
                raise ValueError(f"No metadata found for studyId: {study_id}")
            ancestry = study_ancestries[study_id]
            variants = self._locus_variant_ids(row["locus"])
            if not variants:
                continue
            index = index_by_ancestry[ancestry]
            locus_index = index.filter(index.variantId.isin(variants)).dropDuplicates(
                ["variantId"]
            )
            if locus_index.limit(1).count() == 0:
                continue
            pairwise_datasets.append(
                matrix_by_ancestry[ancestry].get_long_format_ld_matrix(
                    locus_index, ancestry
                )
            )

        output = self._union_pairwise_datasets(locus_set, pairwise_datasets)
        output.dropDuplicates(["ancestry", "variantIdI", "variantIdJ"]).write.mode(
            session.write_mode
        ).parquet(multi_ancestry_pairwise_ld_output_path)

    @staticmethod
    def _normalize_ld_references(
        ld_references: Sequence[Mapping[str, str]],
    ) -> dict[str, dict[str, str]]:
        """Validate and normalize flat LD reference configuration.

        Args:
            ld_references (Sequence[Mapping[str, str]]): Flat LD references.

        Returns:
            dict[str, dict[str, str]]: References keyed by normalized ancestry.
        """
        if not ld_references:
            raise ValueError("At least one LD reference is required")
        normalized: dict[str, dict[str, str]] = {}
        for reference in ld_references:
            missing = {"ancestry", "vi_path", "bm_path"} - reference.keys()
            if missing:
                raise ValueError(
                    f"LD reference is missing required keys: {sorted(missing)}"
                )
            ancestry = normalize_pan_ukbb_population(reference["ancestry"])
            if ancestry in normalized:
                raise ValueError(f"Duplicate LD reference ancestry: {ancestry}")
            normalized[ancestry] = {
                "ancestry": ancestry,
                "vi_path": reference["vi_path"],
                "bm_path": reference["bm_path"],
            }
        return normalized

    @staticmethod
    def _locus_variant_ids(locus: list[Mapping[str, Any]] | None) -> list[str]:
        """Return unique variant IDs from one collected locus array.

        Args:
            locus (list[Mapping[str, Any]] | None): Collected locus variants.

        Returns:
            list[str]: Unique variant IDs in input order.
        """
        if not locus:
            return []
        return list(dict.fromkeys(variant["variantId"] for variant in locus))

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
