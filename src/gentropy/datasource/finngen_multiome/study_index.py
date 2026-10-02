"""Study index for the FinnGen multiome single-cell eQTLs."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import chain

import pyspark.sql.functions as f
from pyspark.sql import Column

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex, StudyType
from gentropy.datasource.finngen_multiome.finemapping import FinnGenMultiomeFinemapping


@dataclass
class FinnGenMultiomeStudyIndex:
    """Study index for the FinnGen multiome single-cell eQTLs.

    One study per cell type and gene with at least one credible set passing the filters.
    Cell types are Azimuth level 1 (including all PBMCs together) and level 2 annotations.
    Level 2 cell types reuse the Cell Ontology terms of the OneK1K studies in Open Targets.
    Sample sizes are the number of donors per cell type (Supplementary Table 12 of the publication).
    """

    # (cell type, Cell Ontology term, number of donors in the eQTL analysis).
    CELL_TYPES: tuple[tuple[str, str, int], ...] = (
        ("l1.B", "CL_0000236", 1103),
        ("l1.CD4_T", "CL_0000624", 1103),
        ("l1.CD8_T", "CL_0000625", 1103),
        ("l1.DC", "CL_0000451", 1095),
        ("l1.Mono", "CL_0000576", 1103),
        ("l1.NK", "CL_0000623", 1103),
        ("l1.PBMC", "CL_2000001", 1103),
        ("l1.other", "CL_0000738", 729),
        ("l1.other_T", "CL_0000084", 1099),
        ("l2.B_intermediate", "CL_0000236", 1097),
        ("l2.B_memory", "CL_0000787", 408),
        ("l2.B_naive", "CL_0000236", 1096),
        ("l2.CD14_Mono", "CL_0002057", 1102),
        ("l2.CD16_Mono", "CL_0002396", 1090),
        ("l2.CD4_CTL", "CL_0000934", 225),
        ("l2.CD4_Naive", "CL_0000624", 1103),
        ("l2.CD4_TCM", "CL_0000904", 1103),
        ("l2.CD4_TEM", "CL_0000905", 1084),
        ("l2.CD8_Naive", "CL_0000625", 1040),
        ("l2.CD8_TEM", "CL_0000913", 1103),
        ("l2.HSPC", "CL_0008001", 307),
        ("l2.ILC", "CL_0001065", 247),
        ("l2.MAIT", "CL_0000940", 1087),
        ("l2.NK", "CL_0000623", 1103),
        ("l2.NK_CD56bright", "CL_0000938", 844),
        ("l2.NK_Proliferating", "CL_0000623", 77),
        ("l2.Plasmablast", "CL_0000980", 175),
        ("l2.Platelet", "CL_0000233", 318),
        ("l2.Treg", "CL_0002677", 1101),
        ("l2.cDC2", "CL_0000451", 1089),
        ("l2.dnT", "CL_0002489", 866),
        ("l2.gdT", "CL_0000798", 311),
        ("l2.pDC", "CL_0000784", 748),
    )

    CONSTANTS = {
        "studyType": StudyType.SCEQTL.value,
        "condition": "naive",
        "hasSumstats": True,
        "publicationTitle": "Population-scale immune multiome atlas reveals regulatory disease mechanisms",
        "publicationJournal": "Nature",
        "publicationDate": "2026",
    }

    @classmethod
    def _cell_type_map(cls: type[FinnGenMultiomeStudyIndex], index: int) -> Column:
        """Map the cell type to one of its annotations.

        Args:
            index (int): 1 for the Cell Ontology term, 2 for the number of donors.

        Returns:
            Column: Annotation of `cellType`.
        """
        return f.create_map(
            *[
                f.lit(x)
                for x in chain(*((row[0], row[index]) for row in cls.CELL_TYPES))
            ]
        )[f.col("cellType")]

    @classmethod
    def from_source(
        cls: type[FinnGenMultiomeStudyIndex],
        session: Session,
        cs_summary_files: str | list[str],
        project_prefix: str,
        summary_stats_location_template: str,
        credset_lbf_threshold: float = 0.8685889638065036,
        purity_min_r2_threshold: float = 0.25,
    ) -> StudyIndex:
        """Build the study index from the credible set summaries.

        Args:
            session (Session): Session object.
            cs_summary_files (str | list[str]): Paths to the `*.SUSIE.cred.tsv.gz` files.
            project_prefix (str): Prefix for the study identifiers, also used as project identifier.
            summary_stats_location_template (str): Location of the cis-nominal summary statistics, with
                `{cell_type}` standing for the cell type, e.g. `l2.CD4_Naive`.
            credset_lbf_threshold (float): Same filter as used for the credible sets.
            purity_min_r2_threshold (float): Same filter as used for the credible sets.

        Returns:
            StudyIndex: One study per cell type and gene.

        Raises:
            ValueError: If a cell type in the input has no annotation.
        """
        studies = (
            FinnGenMultiomeFinemapping.read_cs_summary_files(
                session,
                cs_summary_files,
                credset_lbf_threshold,
                purity_min_r2_threshold,
            )
            .select(
                FinnGenMultiomeFinemapping.extract_cell_type(f.col("trait")).alias(
                    "cellType"
                ),
                f.col("region").alias("geneId"),
            )
            .distinct()
        )
        known = {row[0] for row in cls.CELL_TYPES}
        unknown = {
            r.cellType
            for r in studies.select("cellType").distinct().collect()
            if r.cellType not in known
        }
        if unknown:
            raise ValueError(f"Cell types without annotation: {sorted(unknown)}")

        prefix, suffix = summary_stats_location_template.split("{cell_type}")
        return StudyIndex(
            _df=studies.select(
                FinnGenMultiomeFinemapping.build_study_id(
                    project_prefix, f.col("cellType"), f.col("geneId")
                ).alias("studyId"),
                f.lit(project_prefix).alias("projectId"),
                f.col("geneId"),
                f.col("geneId").alias("traitFromSource"),
                cls._cell_type_map(1).alias("biosampleFromSourceId"),
                cls._cell_type_map(2).cast("integer").alias("nSamples"),
                f.concat(
                    cls._cell_type_map(2).cast("string"), f.lit(" Finnish individuals")
                ).alias("initialSampleSize"),
                f.array(
                    f.struct(
                        cls._cell_type_map(2).cast("integer").alias("sampleSize"),
                        f.lit("Finnish").alias("ancestry"),
                    )
                ).alias("discoverySamples"),
                f.array(f.lit("FinnGen")).alias("cohorts"),
                f.concat(f.lit(prefix), f.col("cellType"), f.lit(suffix)).alias(
                    "summarystatsLocation"
                ),
                *[f.lit(value).alias(key) for key, value in cls.CONSTANTS.items()],
            ).withColumn(
                "ldPopulationStructure",
                StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")),
            ),
            _schema=StudyIndex.get_schema(),
        )
