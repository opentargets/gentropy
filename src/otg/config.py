"""Interface for application configuration."""
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from hail import __file__ as hail_location
from hydra.core.config_store import ConfigStore
from omegaconf import MISSING

defaults_list = ["_self_", {"step": MISSING}]


@dataclass
class SessionConfig:
    """Session configuration."""

    # _target_: str = "otg.common.session.Session"
    write_mode: str = "errorifexists"
    spark_uri: str = "local[*]"
    hail_home: Optional[str] = None
    start_hail: bool = False
    extended_spark_conf: Optional[dict[str, str]] = None
    _target_: str = "otg.common.session.Session"


@dataclass
class StepConfig:
    """Base step configuration."""

    session: SessionConfig = SessionConfig()


@dataclass
class ClumpStep(StepConfig):
    """Clump step configuration."""

    input_path: str = MISSING
    clumped_study_locus_path: str = MISSING
    study_index_path: str | None = field(default=None)
    ld_index_path: str | None = field(default=None)
    locus_collect_distance: int | None = field(default=None)

    _target_: str = "otg.clump.ClumpStep"


@dataclass
class ColocalisationConfig(StepConfig):
    """Colocalisation step configuration."""

    study_locus_path: str = MISSING
    study_index_path: str = MISSING
    coloc_path: str = MISSING
    priorc1: float = 1e-4
    priorc2: float = 1e-4
    priorc12: float = 1e-5
    _target_: str = "otg.colocalisation.ColocalisationStep"


@dataclass
class GeneIndexConfig(StepConfig):
    """Gene index step configuration."""

    target_path: str = MISSING
    gene_index_path: str = MISSING
    _target_: str = "otg.gene_index.GeneIndexStep"


@dataclass
class GWASCatalogIngestionConfig(StepConfig):
    """GWAS Catalog ingestion step configuration."""

    catalog_study_files: list[str] = MISSING
    catalog_ancestry_files: list[str] = MISSING
    catalog_sumstats_lut: str = MISSING
    catalog_associations_file: str = MISSING
    variant_annotation_path: str = MISSING
    catalog_studies_out: str = MISSING
    catalog_associations_out: str = MISSING
    _target_: str = "otg.gwas_catalog_ingestion.GWASCatalogIngestionStep"


@dataclass
class GWASCatalogSumstatsPreprocessConfig(StepConfig):
    """GWAS Catalog sumstat preprocess step configuration."""

    raw_sumstats_path: str = MISSING
    out_sumstats_path: str = MISSING
    _target_: str = (
        "otg.gwas_catalog_sumstat_preprocess.GWASCatalogSumstatsPreprocessStep"
    )


@dataclass
class EqtlCatalogueConfig(StepConfig):
    """eQTL Catalogue step configuration."""

    eqtl_catalogue_paths_imported: str = MISSING
    eqtl_catalogue_study_index_out: str = MISSING
    eqtl_catalogue_summary_stats_out: str = MISSING
    _target_: str = "otg.eqtl_catalogue.EqtlCatalogueStep"


@dataclass
class FinngenStudiesConfig(StepConfig):
    """FinnGen study index step configuration."""

    finngen_study_index_out: str = MISSING
    _target_: str = "otg.finngen_studies.FinnGenStudiesStep"


@dataclass
class FinngenSumstatPreprocessConfig(StepConfig):
    """FinnGen study index step configuration."""

    raw_sumstats_path: str = MISSING
    out_sumstats_path: str = MISSING
    _target_: str = "otg.finngen_sumstat_preprocess.FinnGenSumstatPreprocessStep"


@dataclass
class LDIndexConfig(StepConfig):
    """LD index step configuration."""

    session: SessionConfig = SessionConfig(
        start_hail=True, hail_home=os.path.dirname(hail_location)
    )
    min_r2: float = 0.5
    ld_index_out: str = MISSING
    _target_: str = "otg.ld_index.LDIndexStep"


@dataclass
class LocusToGeneConfig(StepConfig):
    """Locus to gene step configuration."""

    run_mode: str = MISSING
    model_path: str = MISSING
    predictions_path: str = MISSING
    credible_set_path: str = MISSING
    variant_gene_path: str = MISSING
    colocalisation_path: str = MISSING
    study_index_path: str = MISSING
    study_locus_overlap_path: str = MISSING
    gold_standard_curation_path: str = MISSING
    gene_interactions_path: str = MISSING
    features_list: list[str] = field(
        default_factory=lambda: [
            # average distance of all tagging variants to gene TSS
            "distanceTssMean",
            # # minimum distance of all tagging variants to gene TSS
            "distanceTssMinimum",
            # maximum vep consequence score of the locus 95% credible set among all genes in the vicinity
            "vepMaximumNeighborhood",
            # maximum vep consequence score of the locus 95% credible set split by gene
            "vepMaximum",
            # # max clpp for each (study, locus, gene) aggregating over all eQTLs
            # "eqtlColocClppLocalMaximum",
            # # max clpp for each (study, locus) aggregating over all eQTLs
            # "eqtlColocClppNeighborhoodMaximum",
            # # max log-likelihood ratio value for each (study, locus, gene) aggregating over all eQTLs
            # "eqtlColocLlrLocalMaximum",
            # # max log-likelihood ratio value for each (study, locus) aggregating over all eQTLs
            # "eqtlColocLlrNeighborhoodMaximum",
            # # max clpp for each (study, locus, gene) aggregating over all pQTLs
            # "pqtlColocClppLocalMaximum",
            # # max clpp for each (study, locus) aggregating over all pQTLs
            # "pqtlColocClppNeighborhoodMaximum",
            # # max log-likelihood ratio value for each (study, locus, gene) aggregating over all pQTLs
            # "pqtlColocLlrLocalMaximum",
            # # max log-likelihood ratio value for each (study, locus) aggregating over all pQTLs
            # "pqtlColocLlrNeighborhoodMaximum",
            # # max clpp for each (study, locus, gene) aggregating over all sQTLs
            # "sqtlColocClppLocalMaximum",
            # # max clpp for each (study, locus) aggregating over all sQTLs
            # "sqtlColocClppNeighborhoodMaximum",
            # # max log-likelihood ratio value for each (study, locus, gene) aggregating over all sQTLs
            # "sqtlColocLlrLocalMaximum",
            # # max log-likelihood ratio value for each (study, locus) aggregating over all sQTLs
            # "sqtlColocLlrNeighborhoodMaximum",
        ]
    )
    hyperparameters: dict[str, Any] = field(
        default_factory=lambda: {
            "max_depth": 5,
            "loss_function": "binary:logistic",
        }
    )
    wandb_run_name: str | None = None
    perform_cross_validation: bool = False
    _target_: str = "otg.locus_to_gene.LocusToGeneStep"


@dataclass
class OverlapsIndexConfig(StepConfig):
    """Overlaps step configuration."""

    study_locus_path: str = MISSING
    study_index_path: str = MISSING
    overlaps_index_out: str = MISSING
    _target_: str = "otg.overlaps.OverlapsIndexStep"


@dataclass
class PICSConfig(StepConfig):
    """PICS step configuration."""

    study_locus_ld_annotated_in: str = MISSING
    picsed_study_locus_out: str = MISSING
    _target_: str = "otg.pics.PICSStep"


@dataclass
class UKBiobankConfig(StepConfig):
    """UKBiobank step configuration."""

    ukbiobank_manifest: str = MISSING
    ukbiobank_study_index_out: str = MISSING
    _target_: str = "otg.ukbiobank.UKBiobankStep"


@dataclass
class VariantAnnotationConfig(StepConfig):
    """Variant annotation step configuration."""

    session: SessionConfig = SessionConfig(
        start_hail=True, hail_home=os.path.dirname(hail_location)
    )
    variant_annotation_path: str = MISSING
    _target_: str = "otg.variant_annotation.VariantAnnotationStep"


@dataclass
class VariantIndexConfig(StepConfig):
    """Variant index step configuration."""

    variant_annotation_path: str = MISSING
    credible_set_path: str = MISSING
    variant_index_path: str = MISSING
    _target_: str = "otg.variant_index.VariantIndexStep"


@dataclass
class V2GConfig(StepConfig):
    """V2G step configuration."""

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
    interval_sources: Dict[str, str] = field(default_factory=dict)
    v2g_path: str = MISSING
    _target_: str = "otg.v2g.V2GStep"


@dataclass
class Config:
    """Application configuration."""

    # this is unfortunately verbose due to @dataclass limitations
    defaults: List[Any] = field(default_factory=lambda: defaults_list)
    step: StepConfig = MISSING
    datasets: dict[str, str] = field(default_factory=dict)


def register_config() -> None:
    """Register configuration."""
    cs = ConfigStore.instance()
    cs.store(name="config", node=Config)
    cs.store(group="step/session", name="session", node=SessionConfig)
    cs.store(group="step", name="clump", node=ClumpStep)
    cs.store(group="step", name="colocalisation", node=ColocalisationConfig)
    cs.store(group="step", name="eqtl_catalogue", node=EqtlCatalogueConfig)
    cs.store(group="step", name="gene_index", node=GeneIndexConfig)
    cs.store(
        group="step", name="gwas_catalog_ingestion", node=GWASCatalogIngestionConfig
    )
    cs.store(
        group="step",
        name="gwas_catalog_sumstat_preprocess",
        node=GWASCatalogSumstatsPreprocessConfig,
    )
    cs.store(group="step", name="ld_index", node=LDIndexConfig)
    cs.store(group="step", name="locus_to_gene", node=LocusToGeneConfig)
    cs.store(group="step", name="finngen_studies", node=FinngenStudiesConfig)
    cs.store(
        group="step",
        name="finngen_sumstat_preprocess",
        node=FinngenSumstatPreprocessConfig,
    )
    cs.store(group="step", name="overlaps", node=OverlapsIndexConfig)
    cs.store(group="step", name="pics", node=PICSConfig)
    cs.store(group="step", name="ukbiobank", node=UKBiobankConfig)
    cs.store(group="step", name="variant_annotation", node=VariantAnnotationConfig)
    cs.store(group="step", name="variant_index", node=VariantIndexConfig)
    cs.store(group="step", name="variant_to_gene", node=V2GConfig)
