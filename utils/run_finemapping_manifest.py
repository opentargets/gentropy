"""Run the fine-mapping plan generator and manifest generator against GWAS Catalog data.

Usage
-----
    uv run python utils/run_finemapping_manifest.py

Prerequisites
-------------
    gcloud auth application-default login

The script uses two steps in sequence:

1. FineMappingPlanGeneratorStep — reads the StudyIndex and resolves all registered
   constraint sets to produce a fine-mapping plan (runId / studyId / route) written
   as route-partitioned parquet.

2. GWASCatalogFineMappingManifestGenerator — joins the plan with the StudyIndex and
   the resolved summary-statistics paths to produce a single TSV manifest file.

Note on the summary-statistics glob
------------------------------------
The summary statistics live as Spark parquet directories:
    gs://gwas_catalog_inputs/harmonised_summary_statistics/GCST<ID>/

We use the glob ``GCST*`` (NO trailing slash) rather than ``GCST*/``.

Reason: Hadoop's ``FileSystem.globStatus`` interprets a trailing slash as "match
objects *inside* the GCST* directories", which would return individual part files
(many per study). Omitting the slash returns one synthetic directory entry per
study (e.g. ``gs://…/GCST000028``), which is what we want as ``summarystatsLocation``
so that the downstream fine-mapper can pass the path directly to Spark parquet reads.

The TSV manifest is written via ``toPandas().to_csv()``, which uses ``gcsfs`` under
the hood and picks up Application Default Credentials automatically.
"""

from gentropy.common.session import Session
from gentropy.finemapping_manifest import GWASCatalogFineMappingManifestGenerator
from gentropy.finemapping_planner import FineMappingPlanGeneratorStep

# ── Paths ──────────────────────────────────────────────────────────────────────

STUDY_INDEX_PATH = "gs://gwas_catalog_sumstats_susie/study_index"
PLAN_OUTPUT_PATH = "gs://gwas_catalog_multi_ancestry_fine_mapping/plan"
MANIFEST_OUTPUT_PATH = "gs://gwas_catalog_multi_ancestry_fine_mapping/manifest.tsv"

# NO trailing slash — see module docstring for the reason.
SUMSTATS_GLOB = "gs://gwas_catalog_inputs/harmonised_summary_statistics/GCST*"

# ── Session ────────────────────────────────────────────────────────────────────

session = Session(
    add_gcs_connector=True,
    gcs_configuration={"auth_type": "APPLICATION_DEFAULT"},
    write_mode="overwrite",
    log_level="ERROR",
)

# ── Step 1: Fine-mapping plan ──────────────────────────────────────────────────

session.logger.info("Running FineMappingPlanGeneratorStep …")
FineMappingPlanGeneratorStep(
    session=session,
    input_path=STUDY_INDEX_PATH,
    output_path=PLAN_OUTPUT_PATH,
)
session.logger.info(f"Plan written to {PLAN_OUTPUT_PATH}")

# ── Step 2: Fine-mapping manifest ──────────────────────────────────────────────

session.logger.info("Running GWASCatalogFineMappingManifestGenerator …")
GWASCatalogFineMappingManifestGenerator(
    session=session,
    study_index_path=STUDY_INDEX_PATH,
    fine_mapping_planner_path=PLAN_OUTPUT_PATH,
    output_path=MANIFEST_OUTPUT_PATH,
    summary_statistics_glob=SUMSTATS_GLOB,
)
session.logger.info(f"Manifest written to {MANIFEST_OUTPUT_PATH}")
