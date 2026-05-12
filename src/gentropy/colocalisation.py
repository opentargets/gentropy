"""Step to generate colocalisation results."""

from __future__ import annotations

from typing import Annotated, Any

import pyspark.sql.functions as f
from pydantic import BaseModel, Field, field_validator

from gentropy.common.session import Session
from gentropy.dataset.study_locus import FinemappingMethod, StudyLocus
from gentropy.method.colocalisation import ColocalisationMethod

_VALID_COLOC_METHODS = frozenset(m.name for m in ColocalisationMethod)


class ColocalisationDefaults(BaseModel, frozen=True):
    """Defaults for ColocalisationStep.

    All values are frozen - create a new instance to override.
    """

    credible_set_path: Annotated[str, Field(description="Input credible sets path.")]
    coloc_path: Annotated[str, Field(description="Output colocalisation path.")]
    colocalisation_method: Annotated[
        str,
        Field(
            description=(
                "Colocalisation method. One of: "
                + ", ".join(sorted(_VALID_COLOC_METHODS))
                + " (case-insensitive)."
            )
        ),
    ]
    restrict_right_studies: Annotated[
        list[str] | None, Field(description="Restrict right side studies.")
    ] = None
    gwas_v_qtl_overlap_only: Annotated[
        bool, Field(description="Only GWAS vs molQTL overlaps.")
    ] = False
    colocalisation_method_params: Annotated[
        dict[str, Any] | None, Field(description="Method parameters.")
    ] = None

    @field_validator("colocalisation_method", mode="before")
    @classmethod
    def validate_colocalisation_method(cls, v: object) -> object:
        """Validate colocalisation method name.

        Args:
            v: Raw field value.

        Returns:
            object: The original value if valid.

        Raises:
            ValueError: If value is not a recognised colocalisation method.
        """
        if isinstance(v, str) and v.upper() not in _VALID_COLOC_METHODS:
            raise ValueError(
                f"colocalisation_method must be one of "
                f"{sorted(_VALID_COLOC_METHODS)} (case-insensitive), got {v!r}"
            )
        return v


class ColocalisationStep:
    """Colocalisation step.

    This workflow runs colocalisation analyses that assess the degree to which independent signals of the association share the same causal variant in a region of the genome, typically limited by linkage disequilibrium (LD).
    """

    def __init__(
        self,
        config: ColocalisationDefaults,
        session: Session,
    ) -> None:
        """Run Colocalisation step.

        Args:
            config: Step configuration defaults.
            session: Active gentropy session.
        """
        cm = ColocalisationMethod.get_method_class(config.colocalisation_method)
        cs = StudyLocus.from_parquet(session, config.credible_set_path)

        if config.colocalisation_method.upper() == ColocalisationMethod.COLOC.name:
            cs = cs.filter(
                f.col("finemappingMethod").isin(FinemappingMethod.methods_with_lbf())
            )

        overlaps = cs.find_overlaps(
            restrict_right_studies=config.restrict_right_studies,
            gwas_v_qtl_overlap_only=config.gwas_v_qtl_overlap_only,
        )
        params = config.colocalisation_method_params or {}
        result = cm.colocalise(overlapping_signals=overlaps, **params)

        (
            result.df.coalesce(session.output_partitions)
            .write.mode(session.write_mode)
            .parquet(config.coloc_path)
        )
