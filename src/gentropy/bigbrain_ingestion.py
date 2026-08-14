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
    Unlike deCODE, no external reference (e.g. gnomAD) is needed to determine the
    effect allele: the source's `Allele` column names which of `ref`/`alt` the beta
    is relative to, per row.

## Data flow

```mermaid
flowchart TD
  subgraph INPUTS
    A1[full_assoc.tsv.gz @ Zenodo]
    A2[top_assoc.tsv.gz @ Zenodo]
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

    HARM --> O1
    HARM --> O2

    O1 --> QC[SummaryStatisticsQCStep]
    QC --> O3[qc_summary_statistics]

    classDef parquet fill:#bd757c,stroke:#73343A,color:#333
    class A1,A2,P1,P2,O1,O2,O3 parquet
```

??? tip "Inputs"
    - [x] **full_assoc.tsv.gz** — genome-wide SNP-feature association file for the given QTL type, EUR ancestry.
    - [x] **top_assoc.tsv.gz** — per-feature top-hit file; also the source of the sQTL feature-to-gene mapping.

??? tip "Outputs"
    This pipeline produces 2 artefacts per QTL type:

    - [x] **harmonised_summary_statistics** — harmonised summary statistics in Parquet format.
    - [x] **study_index** — study index, one row per feature (gene for eQTL, intron-cluster for sQTL).
"""

from __future__ import annotations

from gentropy.common.session import Session
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
        # outputs
        harmonised_summary_statistics_path: str,
        study_index_path: str,
    ) -> None:
        """Initialise and execute the BigBrain summary-statistics harmonisation step.

        Args:
            session (Session): Active Gentropy Spark session.
            qtl_type (str): QTL type identifier, one of `BigBrainQtlType` values (`"eqtl"`, `"sqtl"`).
            raw_full_assoc_path (str): Path to the raw `full_assoc` Parquet dataset produced
                by `BigBrainSummaryStatisticsIngestionStep`.
            raw_top_assoc_path (str): Path to the raw `top_assoc` Parquet dataset produced
                by `BigBrainSummaryStatisticsIngestionStep`.
            harmonised_summary_statistics_path (str): Destination path for the harmonised
                summary-statistics Parquet dataset, partitioned by `studyId`.
            study_index_path (str): Destination path for the study index Parquet dataset.
        """
        raw = session.spark.read.parquet(raw_full_assoc_path)
        harmonised = BigBrainSummaryStatistics.from_source(raw, qtl_type)

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
