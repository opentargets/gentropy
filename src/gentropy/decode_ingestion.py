r"""deCODE ingestion step.

## Process overview

The ingestion pipeline for the deCODE dataset consists of the following steps, which
must be executed in order:

1. **Obtain an S3 bucket listing** from the deCODE S3 bucket:

    ```bash
        aws s3 ls \
        --recursive \
        --human-readable \
        --summarize  \
        --profile $1 \
        $2 \
        --endpoint-url https://${S3_HOST_URL}:${S3_HOST_PORT} \
        | grep "Proteomics" > manifest.txt
    ```

2. **Generate the manifest** from the bucket listing using `deCODEManifestGenerationStep`.
3. **Ingest protein-complex data** from predicted and experimental files using `MolecularComplexIngestionStep`.
4. **Ingest raw summary statistics** from the `txt.gz` files to Parquet using `deCODESummaryStatisticsIngestionStep`.
5. **Harmonise summary statistics** (including study-index creation and QC) using `deCODESummaryStatisticsHarmonisationStep`.
6. **Transform the pQTL study index** into a standard study index using `pQTLStudyIndexTrasformationStep`.

!!! note "Filtering variants"
    During the harmonisation step, variants with minor allele count (MAC) < 50 and sample size < 30,000
    are discarded to ensure sufficient statistical power and to reduce false positives.

!!! note "Allele flipping"
    The harmonisation step includes allele flipping based on the gnomAD EUR allele frequency:

    - If the allele exists in gnomAD, it is harmonised to the gnomAD allele order.
    - If the allele does not exist in gnomAD, it is kept as-is (no flip).
    - If the allele is strand-ambiguous it is kept as in gnomAD, or retained as-is if not in gnomAD.


!!! note "Effect Allele frequency"
    Because deCODE summary statistics report only the minor allele frequency _m_, the
    effect allele frequency is **inferred using gnomAD EUR allele frequency** _e_:

    ```math
        \begin{aligned}
        d_1 &:= |e - m|, \\
        d_2 &:= |(1 - e) - m|, \\
        \text{effectAlleleFrequencyFromSource} &=
        \begin{cases}
        m, & \text{if } e \text{ is null},\\[4pt]
        m, & \text{if } d_1 \le d_2,\\[4pt]
        1 - m, & \text{otherwise}.
        \end{cases}
        \end{aligned}
    ```

## Data flow

```mermaid
flowchart TD
  subgraph INPUTS
    A1["S3 bucket listing (aws s3 ls)"]
    A2["S3 config (s3_config_path)"]
    A3[AptamerMetadata]
    A4[gnomAD VariantDirection]
    A5[predicted_complex_tab]
    A6[experimental_complex_tab]
    A7[TargetIndex]
  end

  subgraph OUTPUTS
    O1[harmonised_summary_statistics]
    O2[protein_qtl_study_index]
    O3[qc_summary_statistics]
    O4[study_index]
  end

  A1 --> MGEN[deCODEManifestGenerationStep]
  A2 --> MGEN
  MGEN --> P1[manifest]

  A5 --> MCGEN[MolecularComplexIngestionStep]
  A6 --> MCGEN
  MCGEN --> P2[molecular_complex]

  P1 --> INGEST[deCODESummaryStatisticsIngestionStep]
  INGEST --> P3[raw_summary_statistics]

  P3 --> HARM[deCODESummaryStatisticsHarmonisationStep]
  P1 --> HARM
  A3 --> HARM
  A4 --> HARM
  P2 --> HARM

  HARM --> O1
  HARM --> O2
  HARM --> O3

  O2 --> TRANS[pQTLStudyIndexTrasformationStep]
  A7 --> TRANS
  TRANS --> O4

  classDef parquet fill:#bd757c,stroke:#73343A,color:#333
  class A1,A2,A3,A4,A5,A6,A7,P1,P2,P3,O1,O2,O3,O4 parquet
```

??? tip "Inputs"
    - [x] **S3 bucket listing** — text file produced by `aws s3 ls`, required by `deCODEManifestGenerationStep`.
    - [x] **S3 config** — JSON/YAML file with the bucket name used to construct `s3a://` paths.
    - [x] **AptamerMetadata** — SomaScan study table mapping aptamer IDs to gene symbols and UniProt IDs.
    - [x] **gnomAD VariantDirection** — used for allele flipping and EAF inference during harmonisation.
    - [x] **Protein-complex tables** — predicted and experimental files for `MolecularComplexIngestionStep`.
    - [x] **TargetIndex** — Ensembl gene annotations used by `pQTLStudyIndexTrasformationStep`.

??? tip "Outputs"
    This pipeline produces 4 artefacts:

    - [x] **harmonised_summary_statistics** — harmonised summary statistics in Parquet format.
    - [x] **protein_qtl_study_index** — pQTL study index annotated with QC values.
    - [x] **qc_summary_statistics** — per-study summary-statistics QC metrics in Parquet format.
    - [x] **study_index** — standard study index with resolved Ensembl gene IDs.

"""

from __future__ import annotations

from gentropy import (
    Session,
    SummaryStatisticsQC,
    TargetIndex,
)
from gentropy.dataset.molecular_complex import MolecularComplex
from gentropy.dataset.study_index import ProteinQuantitativeTraitLocusStudyIndex
from gentropy.dataset.variant_direction import VariantDirection
from gentropy.datasource.complex_portal import ComplexTab
from gentropy.datasource.decode.aptamer_metadata import AptamerMetadata
from gentropy.datasource.decode.manifest import deCODEManifest
from gentropy.datasource.decode.study_index import deCODEStudyIndex
from gentropy.datasource.decode.summary_statistics import (
    deCODEHarmonisationConfig,
    deCODESummaryStatistics,
)


class deCODEManifestGenerationStep:
    """Build the deCODE manifest Parquet dataset from an ``aws s3 ls`` bucket listing.

    This step should be run **once** after obtaining a fresh listing of the deCODE S3
    bucket.  The resulting manifest is consumed by all downstream ingestion steps.

    See `deCODEManifest.from_bucket_listing` for details on the expected input file format.
    """

    def __init__(
        self,
        session: Session,
        s3_config_path: str,
        bucket_listing_path: str,
        output_path: str,
    ) -> None:
        """Initialise and execute the deCODE manifest generation step.

        Args:
            session (Session): Active Gentropy Spark session.
            s3_config_path (str): Path to the S3 configuration file containing
                the bucket name used to construct fully-qualified ``s3a://`` paths.
            bucket_listing_path (str): Path to the text file produced by
                ``aws s3 ls --recursive --human-readable --summarize``.
            output_path (str): Destination path for the manifest Parquet dataset.
        """
        manifest = deCODEManifest.from_bucket_listing(
            session=session,
            s3_config_path=s3_config_path,
            path=bucket_listing_path,
        )
        manifest.df.repartition(1).write.mode(session.write_mode).parquet(output_path)


class deCODESummaryStatisticsIngestionStep:
    """Ingest deCODE summary statistics from gzipped TSV files into Parquet format.

    Reads all summary-statistics paths recorded in the manifest and converts them
    in parallel (up to `deCODESummaryStatistics.N_THREAD_MAX`
    concurrent threads) from the deCODE S3 bucket into a single partitioned Parquet dataset.

    The step expects the Spark session to be configured with the Hadoop AWS connector
    so that ``s3a://`` paths are accessible.  A representative configuration is shown
    in the example below.

    Examples:
        >>> session = Session(
        ...     spark_uri="yarn",
        ...     extended_spark_conf={
        ...         # Executor
        ...         "spark.executor.memory": "32g",
        ...         "spark.executor.cores": "8",
        ...         "spark.excutor.memoryOverhead": "4g",
        ...         "spark.dynamicAllocation.enabled": "true",
        ...         "spark.sql.files.maxPartitionBytes": "512m",
        ...         # Driver
        ...         "spark.driver.memory": "25g",
        ...         "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ParallelRefProcEnabled -XX:+AlwaysPreTouch",
        ...         "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.367",
        ...         "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        ...         "spark.hadoop.fs.s3a.endpoint": f"https://{credentials.s3_host_url}:{credentials.s3_host_port}",
        ...         "spark.hadoop.fs.s3a.path.style.access": "true",
        ...         "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
        ...         "spark.hadoop.fs.s3a.access.key": f"{credentials.access_key_id}",
        ...         "spark.hadoop.fs.s3a.secret.key": f"{credentials.secret_access_key}",
        ...         # Throughput tuning
        ...         "spark.hadoop.fs.s3a.connection.maximum": "1000",
        ...         "spark.hadoop.fs.s3a.threads.max": "1024",
        ...         "spark.hadoop.fs.s3a.attempts.maximum": "20",
        ...         "spark.hadoop.fs.s3a.connection.timeout": "600000",  # 10min
        ...     },
        ... )  # doctest: +SKIP
    """

    def __init__(
        self,
        session: Session,
        decode_manifest_path: str,
        raw_summary_statistics_path: str,
    ) -> None:
        """Initialise and execute the deCODE summary-statistics ingestion step.

        Args:
            session (Session): Active Gentropy Spark session with S3 connectivity
                configured via the Hadoop AWS connector (see class docstring).
            decode_manifest_path (str): Path to the manifest Parquet dataset produced
                by `deCODEManifestGenerationStep`.
            raw_summary_statistics_path (str): Destination path for the raw summary
                statistics Parquet dataset, partitioned by ``studyId``.
        """
        manifest = deCODEManifest.from_parquet(session, decode_manifest_path)
        summary_statistics_paths = manifest.get_summary_statistics_paths()
        deCODESummaryStatistics.txtgz_to_parquet(
            session=session,
            summary_statistics_list=summary_statistics_paths,
            raw_summary_statistics_output_path=raw_summary_statistics_path,
            n_threads=deCODESummaryStatistics.N_THREAD_MAX,
        )


class MolecularComplexIngestionStep:
    """Ingest predicted and experimental protein-complex data into a `MolecularComplex` Parquet dataset.

    The molecular complex dataset is used during study-index creation to annotate
    multi-protein aptamers with a ``molecularComplexId``.
    """

    def __init__(
        self,
        session: Session,
        predicted_complex_tab_path: str,
        experimental_complex_tab_path: str,
        output_path: str,
    ) -> None:
        """Initialise and execute the molecular complex ingestion step.

        Args:
            session (Session): Active Gentropy Spark session.
            predicted_complex_tab_path (str): Path to the predicted protein-complex
                tab-separated file.
            experimental_complex_tab_path (str): Path to the experimental protein-complex
                tab-separated file.
            output_path (str): Destination path for the merged
                `MolecularComplex` Parquet dataset.
        """
        ComplexTab.from_complex_tab(
            session=session,
            experimental=experimental_complex_tab_path,
            predicted=predicted_complex_tab_path,
        ).coalesce(session.output_partitions).df.write.mode("overwrite").parquet(
            output_path
        )


class deCODESummaryStatisticsHarmonisationStep:
    """Harmonise ingested deCODE summary statistics and generate pQTL study-index and QC outputs.

    The step performs the following operations in sequence:

    1. Builds the `ProteinQuantitativeTraitLocusStudyIndex`
       from the manifest, aptamer metadata, and molecular-complex datasets.
    2. Runs the harmonisation pipeline (`deCODESummaryStatistics.from_source`)
       which includes schema alignment, MAC/sample-size filtering, allele flipping
       against gnomAD EUR AF, EAF inference, sanity filtering, and study-ID update.
    3. Executes `SummaryStatisticsQC` and writes per-study QC metrics to Parquet.
    4. Annotates the pQTL study index with QC results and writes it to Parquet.
    """

    def __init__(
        self,
        session: Session,
        # inputs
        raw_summary_statistics_path: str,
        manifest_path: str,
        aptamer_metadata_path: str,
        variant_direction_path: str,
        molecular_complex_path: str,
        # outputs
        harmonised_summary_statistics_path: str,
        protein_qtl_study_index_path: str,
        qc_summary_statistics_path: str,
        # config
        min_mac_threshold: int = 50,
        min_sample_size_threshold: int = 30_000,
        flipping_window_size: int = 10_000_000,
        pval_threshold: float = 5e-8,
    ) -> None:
        """Initialise and execute the deCODE summary-statistics harmonisation step.

        Args:
            session (Session): Active Gentropy Spark session.
            raw_summary_statistics_path (str): Path to the raw summary-statistics Parquet
                dataset produced by `deCODESummaryStatisticsIngestionStep`.
            manifest_path (str): Path to the manifest Parquet dataset produced by
                `deCODEManifestGenerationStep`.
            aptamer_metadata_path (str): Path to the aptamer metadata TSV file.
            variant_direction_path (str): Path to the gnomAD
                `VariantDirection` Parquet dataset
                used for allele flipping and EAF inference.
            molecular_complex_path (str): Path to the molecular-complex Parquet dataset
                produced by `MolecularComplexIngestionStep`.
            harmonised_summary_statistics_path (str): Destination path for the harmonised
                summary-statistics Parquet dataset, partitioned by ``studyId``.
            protein_qtl_study_index_path (str): Destination path for the pQTL study index
                Parquet dataset annotated with QC results.
            qc_summary_statistics_path (str): Destination path for the per-study QC metrics
                Parquet dataset.
            min_mac_threshold (int): Minimum minor allele count (MAC) required to retain a
                variant. Defaults to 50.
            min_sample_size_threshold (int): Minimum sample size required to retain a variant.
                Defaults to 30,000.
            flipping_window_size (int): Genomic window size (bp) used to partition the
                VariantDirection dataset for the allele-flipping join.  Must match the value
                used when building the VariantDirection dataset. Defaults to 10,000,000.
            pval_threshold (float): P-value threshold used by `SummaryStatisticsQC`.
                Defaults to 1e-6.
        """
        config = deCODEHarmonisationConfig(
            min_mac=min_mac_threshold,
            min_sample_size=min_sample_size_threshold,
            flipping_window_size=flipping_window_size,
        )

        # 1. Produce the PQTLStudyIndex
        m = deCODEManifest.from_parquet(session, manifest_path).persist()
        mc = MolecularComplex.from_parquet(session, molecular_complex_path).persist()
        am = AptamerMetadata.from_source(session, aptamer_metadata_path).persist()
        _pqtl_si = deCODEStudyIndex.from_manifest(m, am, mc).persist()
        m.unpersist()
        am.unpersist()
        mc.unpersist()

        # 2. Produce harmonised summary statistics
        gvd = VariantDirection.from_parquet(session, variant_direction_path).persist()
        rss = session.spark.read.parquet(raw_summary_statistics_path).persist()
        hss, pqtl_si = deCODESummaryStatistics.from_source(rss, gvd, _pqtl_si, config)

        hss.persist()
        pqtl_si.persist()
        rss.unpersist()
        gvd.unpersist()

        hss.df.write.mode(session.write_mode).partitionBy("studyId").option(
            "maxRecordsPerFile", 50_000_000
        ).parquet(
            harmonised_summary_statistics_path
        )

        # 3. Run QualityControl
        hss_qc = SummaryStatisticsQC.from_summary_statistics(
            hss, pval_threshold
        ).persist()
        hss.unpersist()
        hss_qc.df.coalesce(1).write.mode(session.write_mode).parquet(
            qc_summary_statistics_path
        )
        (
            pqtl_si.annotate_sumstats_qc(hss_qc)
            .df.coalesce(1)
            .write.mode(session.write_mode)
            .parquet(protein_qtl_study_index_path)
        )


class pQTLStudyIndexTransformationStep:
    """Transform a `ProteinQuantitativeTraitLocusStudyIndex` into a standard `StudyIndex`.

    This step resolves gene-level annotations from the
    `TargetIndex` (e.g. Ensembl gene IDs) and
    writes a study index compatible with the downstream Open Targets genetics pipeline.
    """

    def __init__(
        self,
        session: Session,
        protein_study_index_path: str,
        study_index_path: str,
        target_index_path: str,
    ) -> None:
        """Initialise and execute the pQTL study-index transformation step.

        Args:
            session (Session): Active Gentropy Spark session.
            protein_study_index_path (str): Path to the
                `ProteinQuantitativeTraitLocusStudyIndex`
                Parquet dataset produced by `deCODESummaryStatisticsHarmonisationStep`.
            study_index_path (str): Destination path for the resolved
                `StudyIndex` Parquet dataset.
            target_index_path (str): Path to the
                `TargetIndex` Parquet dataset used
                to map gene symbols to Ensembl gene IDs.
        """
        pqtl = ProteinQuantitativeTraitLocusStudyIndex.from_parquet(
            session, protein_study_index_path
        )
        ti = TargetIndex.from_parquet(session, target_index_path)

        s = pqtl.to_study(ti)
        s.df.coalesce(1).write.mode(session.write_mode).parquet(study_index_path)
