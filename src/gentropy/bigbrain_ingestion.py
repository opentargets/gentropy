r"""BigBrain ingestion step.

## Process overview

The ingestion pipeline for the BigBrain dataset consists of the following steps, which
must be executed in order, once per QTL type (`eqtl`, `sqtl`):

1. **Download and convert raw files** from their public Zenodo HTTPS URLs to Parquet
   using `BigBrainSummaryStatisticsIngestionStep`.
2. **Harmonise summary statistics** (including study-index creation) using
   `BigBrainSummaryStatisticsHarmonisationStep`.
3. **Run summary-statistics QC** using the generic `SummaryStatisticsQCStep`
   (`gentropy.sumstat_qc_step`) pointed at the harmonised output path — no
   BigBrain-specific QC step is needed.

!!! note "EUR ancestry only, backfilled sample size"
    Only the EUR-ancestry `full_assoc`/`top_assoc` files are ingested (the multi-ancestry
    `_ALL_` files are out of scope). The source reports no per-variant sample size or
    allele frequency; `sampleSize` is therefore backfilled with the constant total
    sample size reported for the whole resource (10,725 donors, shared by eQTL and
    sQTL) rather than a true per-variant N. `effectAlleleFrequencyFromSource` is left
    null.

!!! note "Effect allele resolution"
    `full_assoc` carries no per-row effect-allele indicator (despite one being
    documented in the source README), but `top_assoc`'s `Allele` column equals
    `ref` in all 97,739 real rows checked across both QTL types with zero
    exceptions, so `ref` is treated as the effect allele unconditionally.
    However, source `ref`/`alt` do **not** reliably follow the genome-reference
    convention (empirically, source `ref` equals gnomAD's alternate allele ~99%
    of the time among matched variants), so the harmonisation step still joins
    against a gnomAD `VariantDirection` reference to correct orientation for the
    residual mismatches and drop confirmed allele conflicts. See
    `gentropy.datasource.bigbrain.summary_statistics` for details.

## Data flow

```mermaid
flowchart TD
  subgraph INPUTS
    A1[full_assoc.tsv.gz @ Zenodo]
    A2[top_assoc.tsv.gz @ Zenodo]
    A3[gnomAD VariantDirection]
  end

  subgraph OUTPUTS
    O1[harmonised_summary_statistics]
    O2[study_index]
  end

    A1 --> INGEST[BigBrainSummaryStatisticsIngestionStep]
    A2 --> INGEST
    INGEST --> P1[raw_full_assoc]
    INGEST --> P2[raw_top_assoc]

    P1 --> HARM[BigBrainSummaryStatisticsHarmonisationStep]
    P2 --> HARM
    A3 --> HARM

    HARM --> O1
    HARM --> O2

    O1 --> QC[SummaryStatisticsQCStep]
    QC --> O3[qc_summary_statistics]

    classDef parquet fill:#bd757c,stroke:#73343A,color:#333
    class A1,A2,A3,P1,P2,O1,O2,O3 parquet
```

??? tip "Inputs"
    - [x] **full_assoc.tsv.gz** — genome-wide SNP-feature association file for the given QTL type, EUR ancestry.
    - [x] **top_assoc.tsv.gz** — per-feature top-hit file; also the source of the sQTL feature-to-gene mapping.
    - [x] **gnomAD VariantDirection** — used to correct variant orientation during harmonisation.

??? tip "Outputs"
    This pipeline produces 2 artefacts per QTL type:

    - [x] **harmonised_summary_statistics** — harmonised summary statistics in Parquet format.
    - [x] **study_index** — study index, one row per feature (gene for eQTL, intron-cluster for sQTL).
"""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.variant_direction import DEFAULT_WINDOW_SIZE, VariantDirection
from gentropy.datasource.bigbrain.study_index import BigBrainStudyIndex
from gentropy.datasource.bigbrain.summary_statistics import (
    FULL_ASSOC_SCHEMA,
    BigBrainSummaryStatistics,
)


class BigBrainSummaryStatisticsIngestionStep:
    """Download BigBrain full/top association files from Zenodo and convert them to Parquet.

    The full/top association files are plain public HTTPS downloads (Zenodo), so unlike
    deCODE's S3-listing approach, no bucket credentials are required. See
    `BigBrainSummaryStatistics.download_tsv_gz` for the streaming-download implementation
    and its runtime/disk caveats for the large `full_assoc` files.
    """

    def __init__(
        self,
        session: Session,
        full_assoc_url: str,
        top_assoc_url: str,
        raw_full_assoc_path: str,
        raw_top_assoc_path: str,
    ) -> None:
        """Initialise and execute the BigBrain summary-statistics ingestion step.

        Args:
            session (Session): Active Gentropy Spark session.
            full_assoc_url (str): Public HTTPS URL of the EUR `full_assoc.tsv.gz` file.
            top_assoc_url (str): Public HTTPS URL of the EUR `top_assoc.tsv.gz` file.
            raw_full_assoc_path (str): Destination path for the raw `full_assoc` Parquet dataset.
            raw_top_assoc_path (str): Destination path for the raw `top_assoc` Parquet dataset.
        """
        staged_full_assoc = f"{raw_full_assoc_path}.tsv.gz"
        staged_top_assoc = f"{raw_top_assoc_path}.tsv.gz"

        BigBrainSummaryStatistics.download_tsv_gz(
            full_assoc_url, staged_full_assoc, session
        )
        BigBrainSummaryStatistics.download_tsv_gz(
            top_assoc_url, staged_top_assoc, session
        )

        (
            session.spark.read.csv(
                staged_full_assoc, sep="\t", header=True, schema=FULL_ASSOC_SCHEMA
            )
            .write.mode(session.write_mode)
            .parquet(raw_full_assoc_path)
        )
        (
            session.spark.read.csv(staged_top_assoc, sep="\t", header=True)
            .write.mode(session.write_mode)
            .parquet(raw_top_assoc_path)
        )


class BigBrainSummaryStatisticsHarmonisationStep:
    """Harmonise ingested BigBrain summary statistics and generate the study index.

    The step performs the following operations in sequence:

    1. Runs the harmonisation pipeline (`BigBrainSummaryStatistics.from_source`), which
       includes schema alignment, effect-allele resolution, and sanity filtering.
    2. Builds the feature-to-gene mapping appropriate for the QTL type (direct for
       `eqtl`, joined from `top_assoc` for `sqtl`) and constructs the `StudyIndex`
       via `BigBrainStudyIndex.from_source`.
    """

    def __init__(
        self,
        session: Session,
        qtl_type: str,
        # inputs
        raw_full_assoc_path: str,
        raw_top_assoc_path: str,
        variant_direction_path: str,
        # outputs
        harmonised_summary_statistics_path: str,
        study_index_path: str,
        # config
        flipping_window_size: int = DEFAULT_WINDOW_SIZE,
    ) -> None:
        """Initialise and execute the BigBrain summary-statistics harmonisation step.

        Args:
            session (Session): Active Gentropy Spark session.
            qtl_type (str): QTL type identifier, one of `BigBrainQtlType` values (`"eqtl"`, `"sqtl"`).
            raw_full_assoc_path (str): Path to the raw `full_assoc` Parquet dataset produced
                by `BigBrainSummaryStatisticsIngestionStep`.
            raw_top_assoc_path (str): Path to the raw `top_assoc` Parquet dataset produced
                by `BigBrainSummaryStatisticsIngestionStep`.
            variant_direction_path (str): Path to the gnomAD `VariantDirection` Parquet
                dataset used to correct variant orientation during harmonisation.
            harmonised_summary_statistics_path (str): Destination path for the harmonised
                summary-statistics Parquet dataset, partitioned by `studyId`.
            study_index_path (str): Destination path for the study index Parquet dataset.
            flipping_window_size (int): Genomic window size (bp) used to partition the
                VariantDirection dataset for the orientation-correction join. Must match
                the value used when building the VariantDirection dataset.
        """
        raw = session.spark.read.parquet(raw_full_assoc_path)
        gvd = VariantDirection.from_parquet(session, variant_direction_path)
        harmonised = BigBrainSummaryStatistics.from_source(
            raw, qtl_type, gvd, flipping_window_size
        )

        features = raw.select("feature").distinct()
        if qtl_type == "sqtl":
            top_assoc = session.spark.read.parquet(raw_top_assoc_path)
            feature_gene_map = BigBrainStudyIndex.gene_map_from_top_assoc(top_assoc)
        else:
            feature_gene_map = BigBrainStudyIndex.gene_map_from_feature(features)
        study_index = BigBrainStudyIndex.from_source(
            features, feature_gene_map, qtl_type
        )

        (
            harmonised.df.repartition("studyId")
            .sortWithinPartitions("chromosome", "position", "variantId")
            .write.mode(session.write_mode)
            .partitionBy("studyId")
            .parquet(harmonised_summary_statistics_path)
        )
        study_index.df.coalesce(1).write.mode(session.write_mode).parquet(
            study_index_path
        )
