"""Config management for OTG."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from hydra.core.config_store import ConfigStore
from omegaconf import MISSING


@dataclass
class Config:
    """Configuration for OTG ETL.

    Two parameters are required:
    - step: Step to be run. This is one of the step config classes defined in the config store file.
    - session: Spark session configuration.
    """

    defaults: List[Dict[str, str]] = field(
        default_factory=lambda: [{"step": "???"}, {"session": "session_config"}]
    )


@dataclass
class SessionConfig:
    """ETL config."""

    _target_: str = "otg.common.session.Session"
    app_name: str = "otgenetics"
    spark_uri: str = "local[*]"
    write_mode: str = "overwrite"


@dataclass
class LDIndexStepConfig:
    """LD index step requirements.

    Attributes:
        pop_ldindex_path (str): Input population LD index file from gnomAD.
        ld_radius (int): Window radius around locus.
        grch37_to_grch38_chain_path (str): Path to GRCh37 to GRCh38 chain file.
        ld_index_path (str): Output LD index path.
    """

    _target_: str = "otg.ld_index.LDIndexStep"
    ld_index_raw_template: str = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.ld.variant_indices.ht"
    ld_radius: int = 500_000
    grch37_to_grch38_chain_path: str = MISSING
    ld_index_template: str = MISSING
    ld_populations: List[str] = field(
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
        ]
    )


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
class ColocalisationStepConfig:
    """Colocalisation step requirements.

    Attributes:
        study_locus_path (DictConfig): Input Study-locus path.
        coloc_path (DictConfig): Output Colocalisation path.
        priorc1 (float): Prior on variant being causal for trait 1.
        priorc2 (float): Prior on variant being causal for trait 2.
        priorc12 (float): Prior on variant being causal for traits 1 and 2.
    """

    _target_: str = "otg.colocalisation.ColocalisationStep"
    study_locus_path: str = MISSING
    coloc_path: str = MISSING
    priorc1: float = 1e-4
    priorc2: float = 1e-4
    priorc12: float = 1e-5


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
        lift_over_chain_file_path (str): Path to GRCh37 to GRCh38 chain file.
        approved_biotypes (list[str]): List of approved biotypes.
        anderson_path (str): Anderson intervals path.
        javierre_path (str): Javierre intervals path.
        jung_path (str): Jung intervals path.
        thurnman_path (str): Thurnman intervals path.
        liftover_max_length_difference (int): Maximum length difference for liftover.
        max_distance (int): Maximum distance to consider.
        output_path (str): Output V2G path.
    """

    _target_: str = "otg.v2g.V2GStep"
    variant_index_path: str = MISSING
    variant_annotation_path: str = MISSING
    gene_index_path: str = MISSING
    vep_consequences_path: str = MISSING
    liftover_chain_file_path: str = MISSING
    anderson_path: str = MISSING
    javierre_path: str = MISSING
    jung_path: str = MISSING
    thurnman_path: str = MISSING
    liftover_max_length_difference: int = 100
    max_distance: int = 500_000
    v2g_path: str = MISSING
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


@dataclass
class GWASCatalogStepConfig:
    """GWAS Catalog step requirements.

    Attributes:
        catalog_studies_file (str): Raw GWAS catalog studies file.
        catalog_ancestry_file (str): Ancestry annotations file from GWAS Catalog.
        catalog_sumstats_lut (str): GWAS Catalog summary statistics lookup table.
        catalog_associations_file (str): Raw GWAS catalog associations file.
        variant_annotation_path (str): Input variant annotation path.
        ld_populations (list): List of populations to include.
        min_r2 (float): Minimum r2 to consider when considering variants within a window.
        catalog_studies_out (str): Output GWAS catalog studies path.
        catalog_associations_out (str): Output GWAS catalog associations path.
    """

    _target_: str = "otg.gwas_catalog.GWASCatalogStep"
    catalog_studies_file: str = MISSING
    catalog_ancestry_file: str = MISSING
    catalog_sumstats_lut: str = MISSING
    catalog_associations_file: str = MISSING
    variant_annotation_path: str = MISSING
    min_r2: float = 0.5
    ld_matrix_template: str = MISSING
    ld_index_template: str = MISSING
    ld_populations: List[str] = field(
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
    catalog_studies_out: str = MISSING
    catalog_associations_out: str = MISSING


# Register all configs
def register_configs() -> None:
    """Register step configs - each config class has all the parameters needed to run a step."""
    cs = ConfigStore.instance()
    cs.store(name="config", node=Config)
    cs.store(name="session_config", group="session", node=SessionConfig)
    cs.store(name="locus_to_gene", group="step", node=LocusToGeneConfig)
    cs.store(name="ld_index", group="step", node=LDIndexStepConfig)
    cs.store(name="variant_index", group="step", node=VariantIndexStepConfig)
    cs.store(name="variant_annotation", group="step", node=VariantAnnotationStepConfig)
    cs.store(name="v2g", group="step", node=V2GStepConfig)
    cs.store(name="colocalisation", group="step", node=ColocalisationStepConfig)
    cs.store(name="gwas_catalog", group="step", node=GWASCatalogStepConfig)


# Each of these classes is a config class for a specific step
@dataclass
class VariantAnnotationGnomadConfig:
    """Variant annotation from gnomad configuration."""

    path: str | None = None
    gnomad_file: str = MISSING
    chain_file: str = MISSING
    populations: list = MISSING


@dataclass
class VariantIndexCredsetConfig:
    """Variant index from credible sets configuration."""

    path: str | None = None
    variant_annotation_path: str = MISSING
    credible_sets_path: str = MISSING


class LocusToGeneMode(Enum):
    """Locus to Gene step mode."""

    # TODO: configure them as groups

    TRAIN = "train"
    PREDICT = "predict"


@dataclass
class LocusToGeneConfig:
    """Config for Locus to Gene classifier."""

    run_mode: str = MISSING  # FIXME: define it as LocusToGeneMode
    study_locus_path: str = MISSING
    variant_gene_path: str = MISSING
    colocalisation_path: str = MISSING
    study_index_path: str = MISSING
    study_locus_overlap_path: str = MISSING
    gold_standard_curation_path: str = MISSING
    gene_interactions_path: str = MISSING
    feature_matrix_path: str = MISSING
    features_list: List[str] = MISSING
    hyperparameters: dict = MISSING
    l2g_model_path: Optional[str] = None
    id: str = "locus_to_gene"
    _target_: str = "otg.l2g.LocusToGeneStep"
