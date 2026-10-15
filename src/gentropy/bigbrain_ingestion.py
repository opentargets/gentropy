r"""BigBrain ingestion step.

BigBrain publishes its own SuSiE fine-mapping for its brain eQTL and sQTL meta-analyses
(PolyFun + SuSiE, L=5, in-sample LD from 3,884 European-ancestry donors). This step
ingests it as credible sets, with a study index built from the fine-mapped features, once
per QTL type (`eqtl`, `sqtl`).

??? tip "Inputs"
    - [x] **BigBrain_cis_{eQTL,sQTL}_finemapping.txt.gz**: published fine-mapping, one row
      per credible-set member variant.
    - [x] **BigBrain_cis_sQTL_EUR_top_assoc.tsv.gz** (sQTL only): junction-to-gene mapping.
    - [x] **Target index** (sQTL only): resolves junctions annotated to several genes.

??? tip "Outputs"
    - [x] **credible_set**: `StudyLocus` credible sets.
    - [x] **study_index**: `StudyIndex`, one row per gene (eQTL) or intron junction (sQTL).
"""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.target_index import TargetIndex
from gentropy.datasource.bigbrain import BigBrainQtlType
from gentropy.datasource.bigbrain.finemapping import BigBrainFinemapping
from gentropy.datasource.bigbrain.study_index import BigBrainStudyIndex


class BigBrainFinemappingIngestionStep:
    """Ingest BigBrain's published SuSiE fine-mapping as credible sets and a matching study index."""

    def __init__(
        self,
        session: Session,
        qtl_type: str,
        finemapping_path: str,
        credible_set_path: str,
        study_index_path: str,
        top_assoc_path: str | None = None,
        target_index_path: str | None = None,
    ) -> None:
        """Read the fine-mapping file for one QTL type and write credible sets and a study index.

        The study index is built from the fine-mapped features themselves, so every
        credible set has a study. For sQTL the junction-to-gene mapping comes from the
        `top_assoc` file, and the target index resolves junctions annotated to several
        genes (see `BigBrainStudyIndex.gene_map_from_top_assoc`).

        Args:
            session (Session): Active Gentropy Spark session.
            qtl_type (str): QTL type identifier, one of `BigBrainQtlType` values (`"eqtl"`, `"sqtl"`).
            finemapping_path (str): Path to `BigBrain_cis_{eQTL,sQTL}_finemapping.txt.gz`.
            credible_set_path (str): Destination path for the credible sets.
            study_index_path (str): Destination path for the study index.
            top_assoc_path (str | None): Path to the source `top_assoc` TSV. Required for sQTL.
            target_index_path (str | None): Path to the target index, used to resolve
                multi-gene sQTL junctions. Without it those junctions get no gene.

        Raises:
            ValueError: If `qtl_type` is sQTL and no `top_assoc_path` is given.
        """
        raw = BigBrainFinemapping.read_from_source(session, finemapping_path)
        features = raw.select("feature").distinct()
        if qtl_type == BigBrainQtlType.SQTL.value:
            if top_assoc_path is None:
                raise ValueError("top_assoc_path is required for sQTL fine-mapping ingestion.")
            top_assoc = session.spark.read.csv(top_assoc_path, sep="\t", header=True)
            target_index = (
                TargetIndex.from_parquet(session, target_index_path)
                if target_index_path
                else None
            )
            feature_gene_map = BigBrainStudyIndex.gene_map_from_top_assoc(
                top_assoc, target_index
            )
        else:
            feature_gene_map = BigBrainStudyIndex.gene_map_from_feature(features)

        BigBrainStudyIndex.from_source(
            features, feature_gene_map, qtl_type
        ).df.coalesce(1).write.mode(session.write_mode).parquet(study_index_path)
        BigBrainFinemapping.from_source(raw, qtl_type).df.coalesce(
            session.output_partitions
        ).write.mode(session.write_mode).parquet(credible_set_path)
