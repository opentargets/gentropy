"""Config management for OTG."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from hydra.core.config_store import ConfigStore
from omegaconf import MISSING


@dataclass
class Config:
    """Configuration for otg."""

    defaults: List[Dict[str, str]] = field(
        default_factory=lambda: [{"step": "???"}, {"session": "session_config"}]
    )

    step: Any = MISSING
    session: Any = MISSING


@dataclass
class SessionConfig:
    """ETL config."""

    _target_: str = "otg.common.session.Session"
    app_name: str = "otgenetics"
    spark_uri: str = "local[*]"
    write_mode: str = "overwrite"
    hail_home: Optional[str] = None


@dataclass
class VariantIndexStepConfig:
    """Variant index step requirements.

    Attributes:
        variant_annotation_path (str): Input variant annotation path.
        study_locus_path (str): Input study-locus path.
        variant_index_path (str): Output variant index path.
    """

    _target_: str = "otg.variant_index.VariantIndexStep"
    variant_annotation_path: str = MISSING
    study_locus_path: str = MISSING
    variant_index_path: str = MISSING


@dataclass
class VariantAnnotationStepConfig:
    """Variant annotation step requirements.

    Attributes:
        gnomad_genomes (str): Path to gnomAD genomes hail table.
        chain_38_to_37 (str): Path to GRCh38 to GRCh37 chain file.
        variant_annotation_path (str): Output variant annotation path.
        populations (List[str]): List of populations to include.
    """

    _target_: str = "otg.variant_annotation.VariantAnnotationStep"
    gnomad_genomes: str = MISSING
    chain_38_to_37: str = MISSING
    variant_annotation_path: str = MISSING
    populations: List[str] = field(
        default_factory=lambda: [
            "afr",  # African-American
            "amr",  # American Admixed/Latino
            "ami",  # Amish ancestry
            "asj",  # Ashkenazi Jewish
            "eas",  # East Asian
            "fin",  # Finnish
            "nfe",  # Non-Finnish European
            "mid",  # Middle Eastern
            "sas",  # South Asian
            "oth",  # Other
        ]
    )


@dataclass
class V2GStepConfig:
    """Variant to gene (V2G) step requirements.

    Attributes:
        variant_index_path (str): Input variant index path.
        variant_annotation_path (str): Input variant annotation path.
        gene_index_path (str): Input gene index path.
        vep_consequences_path (str): Input VEP consequences path.
        liftover_chain_file_path (str): Path to GRCh37 to GRCh38 chain file.
        liftover_max_length_difference: Maximum length difference for liftover.
        max_distance (int): Maximum distance to consider.
        approved_biotypes (list[str]): List of approved biotypes.
        intervals (dict): Dictionary of interval sources.
        v2g_path (str): Output V2G path.
    """

    _target_: str = "otg.v2g.V2GStep"
    variant_index_path: str = MISSING
    variant_annotation_path: str = MISSING
    gene_index_path: str = MISSING
    vep_consequences_path: str = MISSING
    liftover_chain_file_path: str = MISSING
    liftover_max_length_difference: int = 100
    max_distance: int = 500_000
    approved_biotypes: List[str] = field(
        default_factory=lambda: [
            "protein_coding",
            "3prime_overlapping_ncRNA",
            "antisense",
            "bidirectional_promoter_lncRNA",
            "IG_C_gene",
            "IG_D_gene",
            "IG_J_gene",
            "IG_V_gene",
            "lincRNA",
            "macro_lncRNA",
            "non_coding",
            "sense_intronic",
            "sense_overlapping",
        ]
    )
    intervals: Dict[str, str] = field(default_factory=dict)
    v2g_path: str = MISSING


# Register all configs
def register_configs() -> None:
    """Register configs."""
    cs = ConfigStore.instance()
    cs.store(name="config", node=Config)
    cs.store(name="session_config", group="session", node=SessionConfig)
