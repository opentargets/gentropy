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
class LDIndexStepConfig:
    """LD matrix step requirements.

    Attributes:
        ld_matrix_template (str): Template path for LD matrix from gnomAD.
        ld_index_raw_template (str): Template path for the variant indices correspondance in the LD Matrix from gnomAD.
        min_r2 (float): Minimum r2 to consider when considering variants within a window.
        grch37_to_grch38_chain_path (str): Path to GRCh37 to GRCh38 chain file.
        ld_populations (List[str]): List of population-specific LD matrices to process.
        ld_index_out (str): Output LD index path.
    """

    _target_: str = "otg.ld_index.LDIndexStep"
    ld_matrix_template: str = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.adj.ld.bm"
    ld_index_raw_template: str = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.ld.variant_indices.ht"
    min_r2: float = 0.5
    grch37_to_grch38_chain_path: str = (
        "gs://hail-common/references/grch37_to_grch38.over.chain.gz"
    )
    ld_populations: List[str] = field(
        default_factory=lambda: [
            "afr",  # African-American
            "amr",  # American Admixed/Latino
            "asj",  # Ashkenazi Jewish
            "eas",  # East Asian
            "fin",  # Finnish
            "nfe",  # Non-Finnish European
            "nwe",  # Northwestern European
            "seu",  # Southeastern European
        ]
    )
    ld_index_out: str = MISSING


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
    ld_index_path: str = MISSING
    min_r2: float = 0.5
    catalog_studies_out: str = MISSING
    catalog_associations_out: str = MISSING


@dataclass
class StudyLocusOverlapStepConfig:
    """StudyLocus overlaps index step requirements.

    Attributes:
        study_locus_path (str): Input study-locus path.
        study_index_path (str): Input study index path to extract the type of study.
        overlaps_index_out (str): Output overlaps index path.
    """

    _target_: str = "otg.overlaps.OverlapsIndexStep"
    study_locus_path: str = MISSING
    study_index_path: str = MISSING
    overlaps_index_out: str = MISSING


@dataclass
class GeneIndexStepConfig:
    """Gene index step requirements.

    Attributes:
        target_path (str): Open targets Platform target dataset path.
        gene_index_path (str): Output gene index path.
    """

    _target_: str = "otg.gene_index.GeneIndexStep"
    target_path: str = MISSING
    gene_index_path: str = MISSING


@dataclass
class GWASCatalogSumstatsPreprocessConfig:
    """GWAS Catalog Sumstats Preprocessing step requirements.

    Attributes:
        raw_sumstats_path (str): Input raw GWAS Catalog summary statistics path.
        out_sumstats_path (str): Output GWAS Catalog summary statistics path.
        study_id (str): GWAS Catalog study identifier.
    """

    _target_: str = (
        "otg.gwas_catalog_sumstat_preprocess.GWASCatalogSumstatsPreprocessStep"
    )
    raw_sumstats_path: str = MISSING
    out_sumstats_path: str = MISSING
    study_id: str = MISSING


@dataclass
class FinnGenStepConfig:
    """FinnGen study table ingestion step requirements.

    Attributes:
        finngen_phenotype_table_url (str): FinnGen API for fetching the list of studies.
        finngen_release_prefix (str): Release prefix pattern.
        finngen_sumstat_url_prefix (str): URL prefix for summary statistics location.
        finngen_sumstat_url_suffix (str): URL prefix suffix for summary statistics location.
        finngen_study_index_out (str): Output path for the FinnGen study index dataset.
        finngen_summary_stats_out (str): Output path for the FinnGen summary statistics.
    """

    _target_: str = "otg.finngen.FinnGenStep"
    finngen_phenotype_table_url: str = MISSING
    finngen_release_prefix: str = MISSING
    finngen_sumstat_url_prefix: str = MISSING
    finngen_sumstat_url_suffix: str = MISSING
    finngen_study_index_out: str = MISSING
    finngen_summary_stats_out: str = MISSING


@dataclass
class UKBiobankStepConfig:
    """UKBiobank study table ingestion step requirements.

    Attributes:
        ukbiobank_manifest (str): UKBiobank manifest of studies.
        ukbiobank_study_index_out (str): Output path for the UKBiobank study index dataset.
    """

    _target_: str = "otg.ukbiobank.UKBiobankStep"
    ukbiobank_manifest: str = MISSING
    ukbiobank_study_index_out: str = MISSING


# Register all configs
def register_configs() -> None:
    """Register configs."""
    cs = ConfigStore.instance()
    cs.store(name="config", node=Config)
    cs.store(name="session_config", group="session", node=SessionConfig)
