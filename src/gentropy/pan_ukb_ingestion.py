"""Step to prepare PanUKBB LD variant indexes and bounded test matrices."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from gentropy.common.session import Session
from gentropy.config import PanUKBBConfig
from gentropy.dataset.variant_index import VariantIndex
from gentropy.datasource.pan_ukbb_ld.ld import (
    PanUKBBLDMatrix,
    normalize_pan_ukbb_population,
)


class PanUKBBVariantIndexStep:
    """Prepare PanUKBB LD variant indexes aligned to Open Targets variant alleles."""

    def __init__(
        self,
        session: Session,
        variant_annotation_path: str,
        pan_ukbb_ht_path: str = PanUKBBConfig().pan_ukbb_ht_path,
        ukbb_annotation_path: str = PanUKBBConfig().ukbb_annotation_path,
        pan_ukbb_pops: Sequence[str] = PanUKBBConfig().pan_ukbb_pops,
        variant_filter_paths: Mapping[str, str] | None = None,
        filtered_ukbb_annotation_path: str | None = None,
    ) -> None:
        """Run the PanUKBB LD reference-preparation step.

        Args:
            session (Session): Session object.
            variant_annotation_path (str): Open Targets variant annotation parquet path.
            pan_ukbb_ht_path (str): PanUKBB Hail variant-table path template with ``{POP}``.
            ukbb_annotation_path (str): Full prepared variant-index output template with ``{POP}``.
            pan_ukbb_pops (Sequence[str]): PanUKBB populations to prepare.
            variant_filter_paths (Mapping[str, str] | None): Optional named variant-set parquet filters.
            filtered_ukbb_annotation_path (str | None): Filtered index output template with ``{POP}`` and ``{FILTER}``.
        """
        variant_annotation = VariantIndex.from_parquet(
            session=session, path=variant_annotation_path
        ).df
        normalized_populations = [
            normalize_pan_ukbb_population(population) for population in pan_ukbb_pops
        ]
        matrix = PanUKBBLDMatrix(
            pan_ukbb_ht_path=pan_ukbb_ht_path,
            ukbb_annotation_path=ukbb_annotation_path,
            ld_populations=normalized_populations,
        )

        for population in normalized_populations:
            matrix.align_ld_index_alleles(
                variant_annotation=variant_annotation,
                population=population,
                hail_table_path=pan_ukbb_ht_path,
                hail_table_output=ukbb_annotation_path,
            )
            prepared_index_path = ukbb_annotation_path.format(POP=population)
            session.logger.info(prepared_index_path)

            if variant_filter_paths:
                self._write_filtered_references(
                    session=session,
                    matrix=matrix,
                    population=population,
                    prepared_index_path=prepared_index_path,
                    variant_filter_paths=variant_filter_paths,
                    filtered_ukbb_annotation_path=filtered_ukbb_annotation_path,
                )

    @staticmethod
    def _write_filtered_references(
        session: Session,
        matrix: PanUKBBLDMatrix,
        population: str,
        prepared_index_path: str,
        variant_filter_paths: Mapping[str, str],
        filtered_ukbb_annotation_path: str | None,
    ) -> None:
        """Write filtered PanUKBB reference indexes.

        Args:
            session (Session): Session object.
            matrix (PanUKBBLDMatrix): PanUKBB LD matrix helper.
            population (str): Normalized PanUKBB population label.
            prepared_index_path (str): Prepared LD variant-index parquet path.
            variant_filter_paths (Mapping[str, str]): Named variant-set parquet filters.
            filtered_ukbb_annotation_path (str | None): Filtered index output template.
        """
        prepared_index = session.spark.read.parquet(prepared_index_path)
        for filter_name, variant_filter_path in sorted(variant_filter_paths.items()):
            variants = session.spark.read.parquet(variant_filter_path)
            filtered_index = matrix.filter_ld_index_to_variants(
                prepared_index, variants
            )

            if filtered_ukbb_annotation_path:
                filtered_index_path = filtered_ukbb_annotation_path.format(
                    POP=population, FILTER=filter_name
                )
                filtered_index.write.mode(session.write_mode).parquet(
                    filtered_index_path
                )
                session.logger.info(filtered_index_path)
